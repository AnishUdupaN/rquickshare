fn main() {
    slint_build::compile("ui/appwindow.slint").unwrap();
    tauri_build::build()
}
