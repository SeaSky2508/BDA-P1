import requests
import json

def main():
    headers = {
        "Content-Type": "application/json",
    }

    params = {
        "number": 10
    }

    url = "http://dog-api.kinduff.com/api/facts"

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        formatted_json = json.dumps(response.json(), indent=4)

        with open("dog_facts.json", "w") as f:
            f.write(formatted_json)
        print("Dog facts data written to 'dog_facts.json'")

    else:
        print("Error:", response.status_code)

if __name__ == '__main__':
    main()
    