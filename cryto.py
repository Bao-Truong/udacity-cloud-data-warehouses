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
# Step2: Store the `key` print above somewhere, you need to use it again. (maybe SSM or Secret Manager on AWS)
# Step3: Set the secretSEED environment with the same value from the `key`
# Step4: We will use that key to descrypt values (such as DB Password, DB Username,...)
# Step5: You may also store the `key` in the .env file, but dont commit that file to public (it is top secret.)

#################


def get_seed():
    load_dotenv()
    seed = os.getenv("secretSEED")
    seed = seed.encode()  # from string to bytes
    return seed


if __name__ == "__main__":
    load_dotenv()
    seed = os.getenv("secretSEED")
    seed = seed.encode()  # from string to bytes

    fernet = Fernet(seed)

    msg = "Message That i want to encrypt, a Password maybe."
    encrypted = fernet.encrypt(msg.encode())
    print("original:", msg)
    print("encrypted (Bytes):", encrypted)
    print("encrypted (String):", encrypted.decode("utf-8"))

    descripted = fernet.decrypt(encrypted).decode()
    print("decrypted:", descripted)
