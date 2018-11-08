extern crate bme280;
extern crate i2cdev;

use bme280::bme280::Bme280;
use i2cdev::linux::LinuxI2CDevice;

fn main() {
    let i2c_addr = 0x76;
    let bus_num = 5;
    let bme = Bme280::<LinuxI2CDevice>::new(i2c_addr, bus_num).unwrap();

    let fahrenheit = bme.read_temperature().unwrap();
    let celsius = (fahrenheit - 32f64) * 5f64 / 9f64;

    println!("Temperature is {} degrees Fahrenheit.", fahrenheit);
    println!("Temperature is {} degrees Celsius.", celsius);
    println!("Barometric pressure is {} inhg.", bme.read_pressure().unwrap());
    println!("Relative Humidity is {}%.", bme.read_humidity().unwrap());
}
