# Project Setup Guide

# Install pip
```sh
python -m pip install --upgrade pip
```

# Install venv
```sh
pip install virtualenv
```

# Create a Virtual Environment
```sh
python -m venv venv
```

or for Python 3:
```sh
python3 -m venv venv
```

# Activate the Virtual Environment
### **For Windows (Command Prompt)**
```sh
venv\Scripts\activate
```
or **PowerShell**:
```sh
venv\Scripts\Activate.ps1
```

### **For macOS/Linux**
```sh
source venv/bin/activate
```

# Install Dependencies
```sh
pip install -r requirements.txt
```

# Run the server
```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```


# Env S3

### Upload to s3 you need to make sure your s3 data in ENV (.env)
```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
AWS_BUCKET_NAME=

```
# Info
#### After install library, add to [requirements.txt](requirements.txt)


# Example api run
```
curl --location 'http://localhost:8000/generate/audience' \--form 'file=@"/path/to/file"'

```