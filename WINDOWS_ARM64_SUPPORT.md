# Windows ARM64 Wheel Support for Ray

This document outlines the changes made to add Windows ARM64 wheel support to the Ray project.

## Summary of Changes

### 1. Python Setup (setup.py)
- Added `get_platform_tag()` function to detect Windows ARM64 architecture
- Modified `BinaryDistribution` class to override platform name for proper wheel tagging
- Wheels will now be tagged as `win_arm64` for ARM64 builds

### 2. Build Scripts
- Created `python/build-wheel-windows-arm64.sh` - ARM64-specific Windows wheel build script
- Added ARM64 architecture detection and Node.js installation handling
- Added platform-specific environment variables for wheel tagging

### 3. Bazel Configuration
- Added `windows_arm64_config` constraint to `bazel/BUILD.bazel`
- Updated `uv_file` selection to include Windows ARM64 support
- Added Windows ARM64 UV binary definition to `WORKSPACE`

### 4. CI/Build System
- Created `.buildkite/windows_arm64.rayci.yml` for Windows ARM64 CI pipeline
- Updated `ci/ray_ci/builder_container.py` to include "arm64" in ARCHITECTURE list
- Modified `ci/ray_ci/windows_builder_container.py` to accept architecture parameter
- Updated `ci/ray_ci/builder.py` to pass architecture to Windows builder
- Created Docker configurations for Windows ARM64 builds

### 5. Docker Support
- Created `ci/docker/windows.arm64.build.wanda.yaml`
- Created `ci/docker/windows.arm64.build.Dockerfile`
- Based on `rayproject/buildenv:windows-arm64` base image

## Key Features

### Architecture Detection
The build system now automatically detects the target architecture and:
- Uses appropriate build scripts
- Sets correct wheel platform tags
- Selects architecture-specific dependencies (UV binary)

### Wheel Platform Tags
Windows ARM64 wheels will be properly tagged as:
- `ray-{version}-cp{python_version}-win_arm64.whl`

### CI Integration
The Windows ARM64 pipeline includes:
- Wheel building for Python 3.9, 3.10, 3.11, 3.12
- Core C++ tests
- Core Python tests
- Serve tests

## Files Created/Modified

### New Files:
- `python/build-wheel-windows-arm64.sh`
- `.buildkite/windows_arm64.rayci.yml`
- `ci/docker/windows.arm64.build.wanda.yaml`
- `ci/docker/windows.arm64.build.Dockerfile`

### Modified Files:
- `python/setup.py` - Added ARM64 platform detection and wheel tagging
- `bazel/BUILD.bazel` - Added Windows ARM64 config setting
- `BUILD.bazel` - Updated UV file selection for Windows ARM64
- `WORKSPACE` - Added Windows ARM64 UV binary definition
- `ci/ray_ci/builder_container.py` - Added "arm64" to ARCHITECTURE list
- `ci/ray_ci/windows_builder_container.py` - Added architecture parameter
- `ci/ray_ci/builder.py` - Updated to pass architecture to Windows builder

## Usage

To build Windows ARM64 wheels locally:
```bash
export BUILD_ONE_PYTHON_ONLY=3.11
./python/build-wheel-windows-arm64.sh
```

To build via CI:
```bash
bazel run //ci/ray_ci:build_in_docker_windows -- wheel --python-version 3.11 --operating-system windows --architecture arm64 --upload
```

## Dependencies

### Requirements:
- Windows ARM64 machine or cross-compilation support
- Node.js with ARM64 support for dashboard build
- Python 3.9+ with ARM64 support
- Bazel with Windows ARM64 cross-compilation support

### External Dependencies:
- UV binary: `uv-aarch64-pc-windows-msvc.zip`
- Redis: Uses existing Windows binaries (architecture-agnostic)
- Base Docker image: `rayproject/buildenv:windows-arm64`

## Notes

1. The Redis configuration already uses `@platforms//os:windows` which works for both x86_64 and ARM64
2. The UV binary is architecture-specific and requires a separate ARM64 build
3. Node.js installation in the build script handles ARM64 compatibility
4. The CI pipeline assumes availability of `windows-arm64` and `builder-windows-arm64` instance types
5. Cross-compilation is not yet implemented - builds require native ARM64 Windows environment

## Testing

The Windows ARM64 CI pipeline includes:
- Wheel building and uploading
- Core C++ test suite
- Core Python test suite
- Ray Serve test suite

All tests exclude `no_windows` tags and ARM64 builds exclude additional architecture-specific tags as needed.
