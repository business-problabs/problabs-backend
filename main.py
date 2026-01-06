
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import resend

# --------------------------------------------------
# App init
# --------------------------------------------------

app = FastAPI(title="ProbLabs Backend")

# --------------------------------------------------
# CORS (keep what you already had)
# --------------------------------------------------

cors_origins = os.getenv("CORS_ORIGINS", "*")
origins = [o.strip() for o in cors_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------
# Root (optional – avoids Render 404 noise)
# --------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "problabs-backend"}

# --------------------------------------------------
# DEBUG: Email test endpoint
# REMOVE AFTER TESTING
# --------------------------------------------------

@app.get("/_debug/test-email")
def test_email():
    resend.api_key = os.getenv("RESEND_API_KEY")

    email_from = os.getenv("EMAIL_FROM")
    reply_to = os.getenv("EMAIL_REPLY_TO")
    app_url = os.getenv("PUBLIC_APP_URL", "https://problabs.net")

    if not resend.api_key:
        raise HTTPException(status_code=500, detail="Missing RESEND_API_KEY")

    if not email_from:
        raise HTTPException(status_code=500, detail="Missing EMAIL_FROM")

    # Set this in Render if you want, otherwise replace inline
    to_email = os.getenv("TEST_TO_EMAIL", "YOUR_PERSONAL_EMAIL@gmail.com")

    try:
        result = resend.Emails.send({
            "from": email_from,
            "to": [to_email],
            "subject": "ProbLabs – Test Email ✅",
            "html": f"""
                <h2>ProbLabs Test Email</h2>
                <p>This confirms email delivery is working.</p>
                <p><a href="{app_url}">{app_url}</a></p>
            """,
            "text": f"ProbLabs test email – {app_url}",
            **({"reply_to": reply_to} if reply_to else {}),
        })

        return {"ok": True, "result": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------
# 👉 PUT YOUR EXISTING ROUTES BELOW THIS LINE
# (lead signup, admin export, etc.)
# --------------------------------------------------

# example placeholder:
# @app.post("/lead")
# def create_lead(...):
#     pass
