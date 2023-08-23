mod utils;
#[cfg(test)]
mod tests {
    use crate::utils::setup;

    #[test]
    fn pytest() {
        setup();
        let o = std::process::Command::new("pytest").output().unwrap();
        println!("stdout: {:?}", o.stdout);
        println!("stderr: {:?}", o.stderr);
        assert!(o.status.success())
    }
}