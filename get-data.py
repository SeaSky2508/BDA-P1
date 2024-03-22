import requests
import json

def main():
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "live_ygddUX1sd9AKit9Dt4HkHMiZTAntInHIGNwpH69amQ1NhuOj3IkCRRj2ABb8bV1y"
    }

    url = "https://api.thedogapi.com/v1/images/search"
    params = {
        "mime_types": "jpg",
        "format": "json",
        "has_breeds": "true",
        "order": "RANDOM",
        "page": "0",
        "limit": "50"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        formatted_json = json.dumps(response.json(), indent=4)

        with open("dog_images.json", "w") as f:
            f.write(formatted_json)
        print("Dog image data written to 'dog_images.json'")

    else:
        print("Error:", response.status_code)
   
if __name__ == '__main__':
    main()