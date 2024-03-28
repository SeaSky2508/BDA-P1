import requests
import json

api_key = 'fNNMCqvKAa8P/7GfdbTa4g==fclQtNTg5I36iK6C'

# Diccionario para almacenar todos los resultados
all_results = []

# Bucle para iterar sobre los valores del parámetro "barking"
for barking_value in range(1,6):
    offset = 0 
    while True:
        api_url = 'https://api.api-ninjas.com/v1/dogs?barking={}'.format(barking_value) +'&offset={}'.format(offset)
        
        # Realizar la consulta a la API
        response = requests.get(api_url, headers={'X-Api-Key': api_key})

        # Verificar el estado de la respuesta
        if response.status_code == requests.codes.ok:
            data = response.json()
            if len(data) == 0:
                break
            
            offset += len(data)
            all_results += data
            
            print(f"Resultados para 'barking' = {barking_value} agregados al diccionario")

        else:
            print("Error:", response.status_code, response.text)
            break

# Escribir todos los resultados en un archivo JSON
print('Total gossos:', len(all_results))
filename = 'dog_caract.json'
with open(filename, 'w') as f:
    json.dump(all_results, f, indent=4)

print(f"Todos los resultados guardados en '{filename}'")
