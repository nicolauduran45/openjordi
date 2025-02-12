# scripts/clean_data.py
import openai

def clean_data():
    with open("data/raw_data.txt", "r") as file:
        raw_data = file.read()
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "Clean and structure grant data."},
                  {"role": "user", "content": raw_data}]
    )
    cleaned_data = response["choices"][0]["message"]["content"]
    with open("data/cleaned_data.txt", "w") as file:
        file.write(cleaned_data)
    print("Data cleaned successfully.")

if __name__ == "__main__":
    clean_data()