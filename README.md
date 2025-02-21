# SCRIPT INFINIX

# First Install
```bash
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