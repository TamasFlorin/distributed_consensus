use serde;
use serde::de;
use std::error;
use std::fs;
use std::io::Write;
use std::path;

pub trait Storage<T: serde::Serialize + de::DeserializeOwned> {
    fn write(&self, value: &T) -> Result<usize, Box<dyn error::Error>>;
    fn read(&self) -> Result<T, Box<dyn error::Error>>;
}

pub struct LocalStorage<P: AsRef<path::Path>> {
    path: P,
}

impl<P: AsRef<path::Path>> LocalStorage<P> {
    pub fn new(path: P) -> Self {
        LocalStorage { path }
    }
}

impl<T: serde::Serialize + de::DeserializeOwned, P: AsRef<path::Path>> Storage<T>
    for LocalStorage<P>
{
    fn write(&self, value: &T) -> Result<usize, Box<dyn error::Error>> {
        let buffer = serde_json::to_string(value)?;
        let mut file = fs::File::create(&self.path)?;
        let bytes_read = file.write(buffer.as_ref())?;
        Ok(bytes_read)
    }

    fn read(&self) -> Result<T, Box<dyn error::Error>> {
        let file = fs::File::open(&self.path)?;
        let value = serde_json::from_reader(file)?;
        Ok(value)
    }
}
