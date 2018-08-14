extern crate chrono;
#[macro_use]
extern crate exonum;
extern crate exonum_time;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

pub mod transactions {
    use exonum::{
        blockchain::{ExecutionError, ExecutionResult, Transaction},
        crypto::PublicKey,
        messages::Message,
        storage::Fork,
    };

    use chrono::{DateTime, Duration, NaiveDateTime, Utc};
    use exonum_time::schema::TimeSchema;

    use schema::{Airplane, AirplaneState, Schema};
    use service::SERVICE_ID;

    #[derive(Debug, Fail)]
    #[repr(u8)]
    pub enum Error {
        #[fail(display = "Airplane already exists")]
        AirplaneAlreadyExists = 0,

        #[fail(display = "Airplane does not exist")]
        AirplaneDoesNotExist = 1,

        #[fail(display = "Transaction is not allowed")]
        TransactionIsNotAllowed = 2,

        #[fail(display = "Engine is not heated")]
        EngineIsNotHeated = 3,
    }

    impl From<Error> for ExecutionError {
        fn from(value: Error) -> ExecutionError {
            let description = format!("{}", value);
            ExecutionError::with_description(value as u8, description)
        }
    }

    transactions! {
        pub AirplaneTransactions {
            const SERVICE_ID = SERVICE_ID;

            struct TxRegisterAirplane {
                pub_key: &PublicKey,

                name: &str,
            }

            struct TxStartTechnicalCheck {
                pub_key: &PublicKey,
            }

            struct TxEndTechnicalCheck {
                pub_key: &PublicKey,

                is_airplane_ok: bool,

                // Total time needed for heating.
                engine_heating_time_seconds: u16,
            }

            struct TxStartFlying {
                pub_key: &PublicKey,
            }

            struct TxEndFlying {
                pub_key: &PublicKey,
            }
        }
    }

    impl Transaction for TxRegisterAirplane {
        fn verify(&self) -> bool {
            self.verify_signature(self.pub_key())
        }

        fn execute(&self, view: &mut Fork) -> ExecutionResult {
            let mut schema = Schema::new(view);

            if schema.airplane(self.pub_key()).is_none() {
                let airplane = Airplane::new(
                    self.pub_key(),
                    self.name(),
                    AirplaneState::WaitingForFlight as u8,
                    AirplaneState::WaitingForFlight.to_string(),
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
                    0,
                );

                schema.airplanes_mut().put(self.pub_key(), airplane);
                Ok(())
            } else {
                Err(Error::AirplaneAlreadyExists)?
            }
        }
    }

    impl Transaction for TxStartTechnicalCheck {
        fn verify(&self) -> bool {
            self.verify_signature(self.pub_key())
        }

        fn execute(&self, view: &mut Fork) -> ExecutionResult {
            let mut schema = Schema::new(view);

            let airplane = schema.airplane(self.pub_key());
            if airplane.is_none() {
                Err(Error::AirplaneDoesNotExist)?
            } else {
                let airplane = airplane.unwrap();
                if airplane.state_number() != AirplaneState::WaitingForFlight as u8 {
                    Err(Error::TransactionIsNotAllowed)?
                } else {
                    let new_airplane = Airplane::new(
                        self.pub_key(),
                        airplane.name(),
                        AirplaneState::TechnicalCheck as u8,
                        AirplaneState::TechnicalCheck.to_string(),
                        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
                        0,
                    );

                    schema.airplanes_mut().put(self.pub_key(), new_airplane);

                    Ok(())
                }
            }
        }
    }

    impl Transaction for TxEndTechnicalCheck {
        fn verify(&self) -> bool {
            self.verify_signature(self.pub_key())
        }

        fn execute(&self, view: &mut Fork) -> ExecutionResult {
            let current_time = TimeSchema::new(&view)
                .time()
                .get()
                .expect("Unexpected error occured while receiving time");

            let mut schema = Schema::new(view);

            let airplane = schema.airplane(self.pub_key());
            if airplane.is_none() {
                Err(Error::AirplaneDoesNotExist)?
            } else {
                let airplane = airplane.unwrap();
                if airplane.state_number() != AirplaneState::TechnicalCheck as u8 {
                    Err(Error::TransactionIsNotAllowed)?
                } else {
                    let airplane_state: AirplaneState;
                    let engine_heating_time_seconds: u16;
                    let start_time: DateTime<Utc>;

                    if self.is_airplane_ok() {
                        airplane_state = AirplaneState::HeatingEngine;
                        engine_heating_time_seconds = self.engine_heating_time_seconds();
                        start_time = current_time;
                    } else {
                        airplane_state = AirplaneState::WaitingForFlight;
                        engine_heating_time_seconds = 0;
                        start_time =
                            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);
                    }

                    let new_airplane = Airplane::new(
                        self.pub_key(),
                        airplane.name(),
                        airplane_state as u8,
                        airplane_state.to_string(),
                        start_time,
                        engine_heating_time_seconds,
                    );

                    schema.airplanes_mut().put(self.pub_key(), new_airplane);

                    Ok(())
                }
            }
        }
    }

    impl Transaction for TxStartFlying {
        fn verify(&self) -> bool {
            self.verify_signature(self.pub_key())
        }

        fn execute(&self, view: &mut Fork) -> ExecutionResult {
            let current_time = TimeSchema::new(&view)
                .time()
                .get()
                .expect("Unexpected error occured while receiving time");
            let mut schema = Schema::new(view);

            let airplane = schema.airplane(self.pub_key());
            if airplane.is_none() {
                Err(Error::AirplaneDoesNotExist)?
            } else {
                let airplane = airplane.unwrap();
                if airplane.state_number() != AirplaneState::HeatingEngine as u8 {
                    Err(Error::TransactionIsNotAllowed)?
                } else {
                    let start_time = airplane.engine_heating_start_time();
                    let substract = current_time - start_time;
                    let min_durarion =
                        Duration::seconds(airplane.engine_heating_time_seconds() as i64);
                    if substract < min_durarion {
                        Err(Error::EngineIsNotHeated)?
                    } else {
                        let new_airplane = Airplane::new(
                            self.pub_key(),
                            airplane.name(),
                            AirplaneState::Flying as u8,
                            AirplaneState::Flying.to_string(),
                            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
                            0,
                        );

                        schema.airplanes_mut().put(self.pub_key(), new_airplane);

                        Ok(())
                    }
                }
            }
        }
    }

    impl Transaction for TxEndFlying {
        fn verify(&self) -> bool {
            self.verify_signature(self.pub_key())
        }

        fn execute(&self, view: &mut Fork) -> ExecutionResult {
            let mut schema = Schema::new(view);

            let airplane = schema.airplane(self.pub_key());
            if airplane.is_none() {
                Err(Error::AirplaneDoesNotExist)?
            } else {
                let airplane = airplane.unwrap();
                if airplane.state_number() != AirplaneState::Flying as u8 {
                    Err(Error::TransactionIsNotAllowed)?
                } else {
                    let new_airplane = Airplane::new(
                        self.pub_key(),
                        airplane.name(),
                        AirplaneState::WaitingForFlight as u8,
                        AirplaneState::WaitingForFlight.to_string(),
                        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
                        0,
                    );

                    schema.airplanes_mut().put(self.pub_key(), new_airplane);

                    Ok(())
                }
            }
        }
    }
}

pub mod schema {
    use exonum::{
        crypto::PublicKey,
        storage::{Fork, MapIndex, Snapshot},
    };

    use chrono::{DateTime, Utc};

    #[derive(Debug, Copy, Clone)]
    #[repr(u8)]
    pub enum AirplaneState {
        WaitingForFlight = 0,

        TechnicalCheck = 1,

        HeatingEngine = 2,

        Flying = 3,
    }

    impl AirplaneState {
        pub fn to_string(&self) -> &str {
            match *self {
                AirplaneState::WaitingForFlight => "Waiting for flight",
                AirplaneState::TechnicalCheck => "Technical check",
                AirplaneState::HeatingEngine => "Heating engine",
                AirplaneState::Flying => "Flying",
            }
        }
    }

    encoding_struct! {
        struct Airplane {
            pub_key: &PublicKey,

            name: &str,

            state_number: u8,

            state_str: &str,

            engine_heating_start_time: DateTime<Utc>,

            /// Total time needed for heating.
            engine_heating_time_seconds: u16,
        }
    }

    #[derive(Debug)]
    pub struct Schema<T> {
        view: T,
    }

    impl<T: AsRef<dyn Snapshot>> Schema<T> {
        pub fn new(view: T) -> Self {
            Schema { view }
        }

        pub fn airplanes(&self) -> MapIndex<&dyn Snapshot, PublicKey, Airplane> {
            MapIndex::new("airplanes", self.view.as_ref())
        }

        pub fn airplane(&self, pub_key: &PublicKey) -> Option<Airplane> {
            self.airplanes().get(pub_key)
        }
    }

    impl<'a> Schema<&'a mut Fork> {
        pub fn airplanes_mut(&mut self) -> MapIndex<&mut Fork, PublicKey, Airplane> {
            MapIndex::new("airplanes", &mut self.view)
        }
    }
}

pub mod service {
    use exonum::{
        api::{self, ServiceApiBuilder, ServiceApiState},
        blockchain::{Service, Transaction, TransactionSet},
        crypto::{Hash, PublicKey},
        encoding::Error as StreamStructError,
        messages::RawTransaction,
        node::TransactionSend,
        storage::Snapshot,
    };

    use schema::{Airplane, Schema};
    use transactions::AirplaneTransactions;

    pub const SERVICE_ID: u16 = 1;
    pub const SERVICE_NAME: &str = "airplane";

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub struct AirplaneQuery {
        pub pub_key: PublicKey,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransactionResponse {
        pub tx_hash: Hash,
    }

    #[derive(Debug, Clone)]
    pub struct AirplaneApi;

    impl AirplaneApi {
        pub fn get_airplane(
            state: &ServiceApiState,
            query: AirplaneQuery,
        ) -> api::Result<Airplane> {
            let snapshot = state.snapshot();
            let schema = Schema::new(snapshot);
            schema
                .airplane(&query.pub_key)
                .ok_or_else(|| api::Error::NotFound("\"Airplane not found\"".to_owned()))
        }

        pub fn post_transaction(
            state: &ServiceApiState,
            query: AirplaneTransactions,
        ) -> api::Result<TransactionResponse> {
            let transaction: Box<dyn Transaction> = query.into();
            let hash = transaction.hash();
            state.sender().send(transaction.into())?;
            Ok(TransactionResponse { tx_hash: hash })
        }

        pub fn wire(builder: &mut ServiceApiBuilder) {
            builder
                .public_scope()
                .endpoint("v1/airplane", Self::get_airplane)
                .endpoint_mut("v1/airplanes/register", Self::post_transaction)
                .endpoint_mut("v1/airplanes/start-tech-check", Self::post_transaction)
                .endpoint_mut("v1/airplanes/end-tech-check", Self::post_transaction)
                .endpoint_mut("v1/airplanes/start-flying", Self::post_transaction)
                .endpoint_mut("v1/airplanes/end-flying", Self::post_transaction);
        }
    }

    #[derive(Debug)]
    pub struct AirplaneService;

    impl Service for AirplaneService {
        fn service_id(&self) -> u16 {
            SERVICE_ID
        }

        fn service_name(&self) -> &'static str {
            SERVICE_NAME
        }

        fn state_hash(&self, _view: &dyn Snapshot) -> Vec<Hash> {
            vec![]
        }

        fn tx_from_raw(
            &self,
            raw: RawTransaction,
        ) -> Result<Box<dyn Transaction>, StreamStructError> {
            let tx = AirplaneTransactions::tx_from_raw(raw)?;
            Ok(tx.into())
        }

        fn wire_api(&self, builder: &mut ServiceApiBuilder) {
            AirplaneApi::wire(builder);
        }
    }
}
