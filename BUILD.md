The project is divided into two parts:

- **core_lib:** This is a Rust library that encompasses all the logic necessary for discovering, connecting to, and transferring files to QuickShare-compatible clients.
- **app/main:** A native Rust application (using Slint for the UI) that utilizes core_lib to handle incoming requests and initiate outgoing ones.

How to build
--------------------------

### core_lib

Building the core_lib is straightforward because it is a basic Rust project.

Install `protobuf-compiler` system package, and then simply run `cargo build` or `cargo build --release` from `core_lib` folder.

### app/main

The app/main is developed as a native Rust + Slint application.

(all commands are run inside the `app/main/src-tauri` folder)

Building the app/main on Linux requires some system dependencies:

```
sudo apt-get install libglib2.0-dev libgtk-3-dev libwebkit2gtk-4.1-dev protobuf-compiler
```

- To run the debug version:

```
cargo run
```

- To build a release package:

```
cargo build --release
```
