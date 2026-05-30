use centaurus::db::init::Connection;

pub trait DBTrait {}

impl DBTrait for Connection {}
