# MADS SIADS 699: Capstone Project - 

By [Cyril Scerbin](mailto:kshcherb@umich.edu), [Jason Harris](mailto:harjason@umich.edu), [Emily Lerner](mailto:eslerner@umich.edu)

-----
<details>
<summary>Prerequisites</summary>

Before completing `Local Development Setup`, ensure your machine meets the following software requirements:

### Required Software
* **[Docker Desktop](https://www.docker.com/products/docker-desktop)**: Required to host the development containers.
    * *Windows Users:* Ensure **WSL2** is installed and updated.
* **[Visual Studio Code](https://code.visualstudio.com/)**: The recommended IDE for this project.
* **[Dev Containers Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)**: Allows VS Code to open the project inside the Docker environment.

### Recommended Tools
* **[direnv](https://direnv.net/)**: For automatic loading of environment variables from your `.env` file.
* **[Git](https://git-scm.com/)**: To clone and manage project versions.

</details>

<details>
<summary>Local Development Setup</summary>

Before following this setup, please see the `Prerequisites` section.

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
<summary>Common Issues (Check this if your code will not run) </summary>

## Adjusting Docker Desktop Resource Limits

If your project is crashing with `Out of Memory` (OOM) errors, you may need to manually increase the RAM and CPU allocated to Docker.

### For macOS and Linux
1. Open **Docker Desktop**.
2. Click the **Settings** (gear icon) in the top-right corner.
3. Select **Resources** from the left-hand sidebar.
4. Under **Advanced**, use the sliders to adjust:
   - **CPUs**: Set to at least 8 (or half your total cores).
   - **Memory**: Increase to **60GB** (if your hardware allows).
5. Click **Apply & Restart**.

### For Windows (WSL2 Backend)
In the modern WSL2 mode, Docker Desktop does not use sliders. Instead, it "borrows" memory from the global WSL2 utility VM.

1. **Check if sliders are missing:** Go to `Settings > Resources`. If you see a message saying *"You are using the WSL 2 backend"*, the sliders will be hidden.
2. **The Fix:** You must use the `.wslconfig` method:
   - Press `Win + R`, type `%UserProfile%`, and press Enter.
   - Edit (or create) the `.wslconfig` file.
   - Add/Update these lines:
     ```ini
     [wsl2]
     memory=60GB
     processors=16
     ```
3. **Restart:** You **must** run `wsl --shutdown` in PowerShell for these changes to take effect.

> [!TIP]
> **Pro-Tip for Mac (Apple Silicon):** Ensure **"Use Virtualization framework"** and **"VirtioFS"** are enabled under `Settings > General` and `Resources > File Sharing` for a 10x boost in file-reading speed within your notebooks.

</details>
