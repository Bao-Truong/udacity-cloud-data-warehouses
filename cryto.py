from dotenv import load_dotenv
import os
from cryptography.fernet import Fernet

### IMPORTANT ###
# Step0: Install the missing library of Python (python -m pip install -r requirements.txt)
# Step1: Get your private Fernet Key, copy and run the code bellow (3 lines)
#########################################
# from cryptography.fernet import Fernet
# key = Fernet.generate_key()
# print(key.decode("utf-8"))
#########################################
# Step2: Store the `key` print above somewhere, you need to use it again. (maybe SSM or Secret Manager on AWS, here i will use .env file)
# Step3: Set the secretSEED environment with the value from the `key` (or inside .env file, exp: secretSEED=ABCDEawegwaeg_==)
# Step4: You can use this python script to encrypt values and store it into the dwh.cfg (replace the msg in line 36 to the value you want to encrypt)
# Step5: Use this script to encrypt all the sensitive data (such as DB Password, DB Username, DB HOST, DB Name)
# Step6: You may also store the `key` in the .env file, but dont commit that file to public (it is top secret.)

#################


def get_seed():
    """Read the secret Key from environment for Fernet Algo

    Returns:
        binary: the key used to enscrypt/decrypt the sensitive data
    """
    load_dotenv()
    seed = os.getenv("secretSEED")
    seed = seed.encode()  # from string to bytes
    return seed


if __name__ == "__main__":
    seed = get_seed()

    fernet = Fernet(seed)

    # CHANGE HERE, put in the sensitive data you want to encrypt
    msg = "Message That i want to encrypt, a Password maybe."
    encrypted = fernet.encrypt(msg.encode())
    print("original:", msg)
    print("encrypted (Bytes):", encrypted)
    print("encrypted (String):", encrypted.decode("utf-8"))

    descripted = fernet.decrypt(encrypted).decode()
    print("decrypted:", descripted)
