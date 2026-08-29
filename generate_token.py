import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
REDIRECT_URI = os.getenv("FYERS_REDIRECT_URI")

def generate_token():
    session = fyersModel.SessionModel(
        client_id=CLIENT_ID,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code"
    )
    
    auth_url = session.generate_authcode()
    print("\n1. Open this URL in your browser:\n")
    print(auth_url)
    print("\n2. Log in and authorize. You will be redirected to your Redirect URI.")
    print("3. Copy the 'auth_code' parameter value from the browser URL address bar.\n")
    
    auth_code = input("Paste auth_code here: ").strip()
    
    session.set_token(auth_code)
    response = session.generate_token()
    
    if response.get("s") == "ok":
        access_token = response["access_token"]
        with open("access_token.txt", "w") as f:
            f.write(access_token)
        print("\nSuccess! Token saved to access_token.txt")
    else:
        print(f"\nFailed to generate token: {response}")

if __name__ == "__main__":
    generate_token()
