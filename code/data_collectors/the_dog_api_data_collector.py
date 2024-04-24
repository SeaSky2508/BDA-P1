import requests
import json

def main():

    #### Obtenim la llista de races
    breedsList_url="https://api.thedogapi.com/v1/breeds"
    response = requests.get(breedsList_url)

    ### En cas d'èxit, la guardem en una variable
    if response.status_code == 200:
        breeds_list = response.json()
        for breed in breeds_list:
            del breed["weight"]
            del breed["height"]
        
        with open("dog_caract2.json", "w") as f:
            breeds_list = json.dumps(breeds_list, indent=4)
            f.write(breeds_list)

    ### En qualsevol altre cas, avisem que no ha funcionat
    else:
        print("Failed to download the file.")
    




    #### Query a la api 

    #### AL FINAL NO HO FAREM SERVIR PERQUÈ NO ENS CALEN LES IMATGES D'AQUESTA API
    #### EN TENIM PROU AMB LA LLISTA DE RACES (Obtinguda a sobre)

    ### Definim url, paràmetres i clau de l'api
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
        "limit": "500"
    }

    ### Fem la query amb els requisits definits
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