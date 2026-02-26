# Lab 2

## Team Member

Olivie Bergeron #41068227 - Initial repo and codebase setup, and demoed the local env and how it works

## Local Setup

### Step 1: Install or Update Azure Functions Core Tools

1. In Visual Studio Code, press F1 to open the Command Palette

2. Search for and run: Azure Functions: Install or Update Core Tools

3. Wait for the installation to complete

### Step 2: Create a Virtual Environment

```
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install Dependencies

```
pip install -r requirements.txt
```

### Step 4: Start/install Azurite (using npm as package manager)

Install:

```bash
npm install -g azurite
```

Start:

```
azurite --silent --location .azurite --debug .azurite/debug.log
```

### Step 5: Run the Function

```
func start
```

OR Press F5 key

### local settings

```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "PDFStorageConnection": "UseDevelopmentStorage=true"
  },
  "Host": {
    "CORS": "*"
  }
}
```

## Youtube video

- Part 1: https://youtu.be/HDBiuC5psAU
- Part 2:
- Part 3: https://www.youtube.com/watch?v=4gEN4qbHRpQ
