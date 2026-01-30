import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("country_revenue.csv")

plt.bar(df["country"], df["revenue"])
plt.xticks(rotation=90)
plt.title("Revenue by Country")
plt.tight_layout()
plt.show()
