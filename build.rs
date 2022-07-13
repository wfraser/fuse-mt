use std::env;
use std::fs;
use std::path::Path;
use std::io::Write;

fn main() {
    let buf = fs::read_to_string("Cargo.lock").unwrap();
    let cargo = buf.parse::<toml::Value>().unwrap();
    println!("{:#?}", cargo);

    let ver_path = Path::new(&env::var_os("OUT_DIR").unwrap()).join("ver.include");
    let mut ver_file = fs::File::create(&ver_path).unwrap();

    for package in cargo.get("package").unwrap().as_array().unwrap() {
        let package = package.as_table().unwrap();
        if package.get("name").and_then(toml::Value::as_str) == Some("fuser") {
            write!(ver_file, "pub const FUSER_VER: &str = {};", package.get("version").unwrap()).unwrap();
            break;
        }
    }
}
