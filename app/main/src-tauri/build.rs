fn main() {
    println!("cargo:rerun-if-changed=ui/appwindow.slint");
    slint_build::compile("ui/appwindow.slint").unwrap();
}
