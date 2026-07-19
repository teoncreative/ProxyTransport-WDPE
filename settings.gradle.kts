rootProject.name = "ProxyTransport"

// Shared ProxyTransport wire implementation, vendored as a git submodule (see .gitmodules).
// Run `git submodule update --init` after cloning.
include(":common")
project(":common").projectDir = file("common")
