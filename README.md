# MADS SIADS 699: Capstone Project - 

By [Cyril Scerbin](mailto:kshcherb@umich.edu), [Jason Harris](mailto:harjason@umich.edu), [Emily Lerner](mailto:eslerner@umich.edu)

-----

<details>
<summary>Local Development Setup</summary>

This project uses Dev Containers in Visual Studio Code and Docker Desktop to provide a reproducible and isolated development environment.

Before starting, ensure you have the following installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (required to run containers)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Dev Containers extension for VS Code](https://code.visualstudio.com/docs/devcontainers/containers)

## Clone the Repository
First, fetch the project from GitHub:
```sh
git clone https://github.com/shcherbin/mads-siads-699-winter-2026-capstone.git
```

## Configuration
All configuraion parameters are stored in the `.env` file. 
Once the repo is cloned copy the `.env.example` and made the necessary changes.

```sh
cp .env.example .env
```

to access configration parameters in python use the following code snippet. 

```python
from birds.settings import load_settings

settings = load_settings()

print(settings.env)
print(settings.version)
```

## Accessing .env in the Terminal
Enable automatic environment variable loading with direnv:
```sh
direnv allow
```
Run this once per shell. If .env changes, you’ll be prompted to re-run the command.

Note: If you are using a windows pc and WSL, you may need to run the following for the direnv file to be valid:
```
sudo apt install dos2unix
dos2unix .envrc
```
If using windows, you may also want to do the following to ensure consistent spacing in proceeding pr's:
```
git config --global core.autocrlf input
```


## Start the Development Environment
With Docker Desktop running, open the repository in VS Code. You should see a prompt to “Reopen in Container”.
Alternatively, use the Command Palette (Ctrl+Shift+P / Cmd+Shift+P) and select:
```
Dev Containers: Reopen in Container
```
This builds and runs the project’s development container, installing all dependencies inside an isolated environment.

</details>

<details>
<summary>Recommended Hardware Minimums</summary>

| Component | Minimum Requirement | Recommended (for 60GB WSL config) |
| :--- | :--- | :--- |
| **Processor (CPU)** | 8 Cores (Intel i7 / Ryzen 7 / Apple M2) | 16 Cores (Intel i9 / Ryzen 9 / Apple Max) |
| **Memory (RAM)** | 32 GB | 64 GB |
| **Storage** | 50 GB Free SSD Space | 200 GB Free SSD Space |
| **OS** | Windows 11 (WSL2) / macOS / Linux | Linux (Ubuntu) or Windows 11 (WSL2) |
| **Virtualization** | BIOS Virtualization Enabled | BIOS Virtualization Enabled |

</details>


<details>
<summary>Common Issues</summary>

If you are on windows and your notebooks do not run, 
adjust the WSL memory size 
```
notepad.exe %UserProfile%/.wslconfig
```
and add the following:
```
[wsl2]
memory=60GB #as allows on your machine.
processors=16
swap=0
```
then run:
```
wsl --shutdown
```
</details>
