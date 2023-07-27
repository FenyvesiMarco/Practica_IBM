from flask import Flask, render_template
from pyspark.sql import SparkSession

app = Flask(__name__)

spark = SparkSession.builder.config("spark.ui.port", "4040").config("spark.jars", "mysql-connector-java-8.0.18.jar").getOrCreate()

def get_results():
    df = spark.read.csv("Erasmus.csv", header=True)

    d3 = df.groupBy(["Receiving Country Code", "Sending Country Code"]).count()

    d4 = d3.orderBy(["Receiving Country Code", "Sending Country Code"])
    results = d4.filter((d4["Receiving Country Code"] == "IT") | (d4["Receiving Country Code"] == "MK") | (
            d4["Receiving Country Code"] == "RO")).collect()

    results_list = []
    for row in results:
        results_list.append({
            "Receiving Country Code": row["Receiving Country Code"],
            "Sending Country Code": row["Sending Country Code"],
            "Count": row["count"]
        })

    print("Results List:", results_list)

    return results_list


@app.route('/')
def display_results():
    results = get_results()
    return render_template('results.html.txt', results=results)

if __name__ == '__main__':
    app.run(debug=True)
