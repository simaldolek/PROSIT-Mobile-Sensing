import rncryptor as rn
import base64
import logging
from timezonefinder import TimezoneFinder

def encrypt_data(data, encryption_key):
    try:
        encrypted_data = base64.b64encode(rn.encrypt(data, encryption_key))
        del encryption_key  # Remove the key from memory
        return encrypted_data
    except Exception as e:
        logging.error(f"Encryption failed: {str(e)}")
        return None
    
def decrypt_data(encrypted_data, decryption_key):
    try:
        # Ensure that the encrypted_data is decoded from base64 to bytes
        decrypted_data = rn.decrypt(base64.b64decode(encrypted_data), decryption_key)
        del decryption_key  # Remove the key from memory
        return decrypted_data  # This should return bytes
    except Exception as e:
        logging.error(f"Decryption failed: {str(e)}")
        return None
    
def get_timezone(latitude, longitude):
    # print(f"Getting timezone for coordinates: Latitude = {latitude}, Longitude = {longitude}")
    try:
        # Convert latitude and longitude to float
        latitude = float(latitude)
        longitude = float(longitude)
    except ValueError:
        # logging.error(f"Error: Invalid latitude or longitude. Cannot convert '{latitude}', '{longitude}' to float.")
        return "Unknown_Timezone"

    # Derive timezone information from latitude and longitude
    tf = TimezoneFinder()
    timezone = tf.timezone_at(lng=longitude, lat=latitude)
    
    if timezone:
        # print(f"Timezone found: {timezone}")
        return timezone
    else:
        # print(f"Timezone not found for coordinates: Latitude = {latitude}, Longitude = {longitude}")
        logging.error(f"Error: No timezone found for coordinates: Latitude = {latitude}, Longitude = {longitude}")
        return "Unknown_Timezone"


if __name__ == "__main__":
    # Test encryption and decryption
    encryption_key = b"supersecret"
    data = "Hello, world!"
    
    encrypted_data = encrypt_data(data, encryption_key)
    if encrypted_data:
        decrypted_data = decrypt_data(encrypted_data, encryption_key)

        print(f"Original data: {data}")
        print(f"Encrypted data: {encrypted_data}")
        print(f"Decrypted data: {decrypted_data}")

        if decrypted_data is not None:
            assert data == decrypted_data, "Decryption failed"
            print("Decryption successful.")
        else:
            print("Decrypted data is None.")
    else:
        print("Encryption failed.")
