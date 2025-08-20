import subprocess
import psycopg2
import os
import shlex
import tempfile

# PSQL database connection
conn = psycopg2.connect(
    dbname="cmm_compounds_structures",
    user="pilarbourg",
    password="",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# Only gets compounds within the specified ID range
cursor.execute("""
    SELECT compound_id, smiles
    FROM compound_identifiers
    WHERE mol2 IS NULL
      AND (smiles IS NOT NULL AND smiles != '')
      AND compound_id BETWEEN 30001 AND 40000 # SPECIFY ID RANGE
    LIMIT 10000
""")
rows = cursor.fetchall()

print(f"Found {len(rows)} compounds to process")

for row in rows:
    compound_id, smiles = row
    try:
        if not smiles:
            continue

        print(f"Processing compound {compound_id}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mol2") as tmpfile:
            output_file = tmpfile.name

        escaped_smiles = shlex.quote(smiles)
        command = f"obabel -:{escaped_smiles} -O {output_file} --gen3D"

        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30  # Avoid getting stuck
        )

        if "Could not correct" in result.stderr:
            print(f"Skipping compound {compound_id} due to stereochemistry issue.")
            os.remove(output_file)
            continue

        with open(output_file, 'r') as file:
            mol2_data = file.read()

        os.remove(output_file)

        cursor.execute("UPDATE compound_identifiers SET mol2 = %s WHERE compound_id = %s", (mol2_data, compound_id))
        conn.commit()
        print(f"Updated compound {compound_id}")

    except subprocess.TimeoutExpired:
        print(f"⏱️ Timeout for compound {compound_id}, skipping.")
        if os.path.exists(output_file):
            os.remove(output_file)
    except subprocess.CalledProcessError as e:
        print(f"Subprocess error for compound {compound_id}: {e.stderr}")
        if os.path.exists(output_file):
            os.remove(output_file)
    except Exception as e:
        print(f"General error for compound {compound_id}: {e}")
        if os.path.exists(output_file):
            os.remove(output_file)