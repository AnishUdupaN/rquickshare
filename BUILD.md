The project is divided into two parts:

- **core_lib:** This is a Rust library that encompasses all the logic necessary for discovering, connecting to, and transferring files to QuickShare-compatible clients.
- **app/main:** A native Rust desktop application built with [Slint UI](https://slint.dev/) that utilizes `core_lib` to handle incoming requests and initiate outgoing transfers.

Prerequisites
--------------------------

Make sure you have the following installed:

- **Rust toolchain** (stable) with Cargo: `rustup default stable`
- **protobuf-compiler** (`protoc`) for compiling protocol buffers.
- Linux system dependencies (for building UI, font management, and system notifications):
  - **Ubuntu / Debian:**
    ```bash
    sudo apt-get update
    sudo apt-get install -y protobuf-compiler libdbus-1-dev libfontconfig1-dev pkg-config
    ```
  - **Fedora / RHEL:**
    ```bash
    sudo dnf install -y protobuf-compiler dbus-devel fontconfig-devel pkgconf-pkg-config
    ```
  - **Arch Linux:**
    ```bash
    sudo pacman -S protobuf dbus fontconfig pkgconf
    ```


How to build
--------------------------

Because the repository is organized as a Cargo workspace, building the entire project (both `core_lib` and the desktop UI application `rquickshare`) can be done directly with `cargo` from the project root.

### Build everything (Whole Project)

Run the following from the root of the repository:

- **Debug build:**
  ```bash
  cargo build
  ```

- **Release build:**
  ```bash
  cargo build --release
  ```

This automatically compiles both the backend (`core_lib`) and the frontend Slint UI application (`rquickshare`).
The compiled application binary will be located at:
```
target/release/rquickshare
```

### Run the App

To run the desktop application directly:

```bash
cargo run -p rquickshare
```

### Build individual packages (Optional)

You can also target specific members of the workspace:

- **core_lib only:**
  ```bash
  cargo build -p rqs_lib
  # or from core_lib directory:
  cd core_lib && cargo build
  ```

- **Slint desktop app only:**
  ```bash
  cargo build -p rquickshare
  ```

For Slint backend configuration, rendering options (such as Qt, Skia, or FemtoVG/software renderers), and platform notes, refer to the [Slint Documentation](https://slint.dev/docs).


