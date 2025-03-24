import pandas as pd
import smtplib
import schedule
import time
from email.message import EmailMessage
from google.cloud import bigquery
from google.oauth2 import service_account
from config import EMAIL_PASSWORD, dataset_id
import os

# Initialize BigQuery client
client = bigquery.Client()
datasetId = dataset_id

# Function to fetch data
def fetch_data(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

# Generate Yearly or Monthly Report
def generate_report(report_type="yearly"):
    if report_type == "yearly":
        query = f"SELECT * FROM `{client.project}.{datasetId}.kpi_yearly_report` WHERE year = EXTRACT(YEAR FROM CURRENT_DATE())"
        filename = "Yearly_Financial_Report.xlsx"
    elif report_type == "monthly":
        query = f"SELECT * FROM `{client.project}.{datasetId}.agg_monthly_stock_performance` WHERE EXTRACT(MONTH FROM month) = EXTRACT(MONTH FROM CURRENT_DATE())"
        filename = "Monthly_Stock_Performance_Report.xlsx"
    else:
        raise ValueError("Invalid report type. Use 'yearly' or 'monthly'.")

    df = fetch_data(query)

    if df.empty:
        print(f"No data found for {report_type} report. Skipping email.")
        return None

    df.to_excel(filename, index=False)
    return filename

# Function to get PNG image files for email attachments
def get_graph_images():
    image_folder = "graphs"  # Update this with your actual folder containing images
    image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder) if f.endswith(".png")]
    return image_files

# Function to send email with report and graphs
def send_email(report_file, report_type):
    if report_file is None:
        print(f"No {report_type} report generated. Email not sent.")
        return

    sender_email = "psspsp7@gmail.com"
    password = EMAIL_PASSWORD
    receiver_emails = ["pavansaipatel.2002@gmail.com"]

    subject = f"Automated {report_type.capitalize()} Report"
    body = f"Attached is the {report_type.capitalize()} Report for your review.\n\nGraphs are also attached."

    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_emails)
    msg["Subject"] = subject
    msg.set_content(body)

    # Attach financial report
    with open(report_file, "rb") as attachment:
        msg.add_attachment(attachment.read(), maintype="application", subtype="octet-stream", filename=report_file)

    # Attach visualization graphs
    graph_files = get_graph_images()
    for graph in graph_files:
        with open(graph, "rb") as img:
            msg.add_attachment(img.read(), maintype="image", subtype="png", filename=os.path.basename(graph))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        print(f"Email sent successfully to {', '.join(receiver_emails)}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Automate Scheduled Email Sending
def schedule_reports():
    schedule.every(30).days.do(lambda: send_email(generate_report("monthly"), "monthly"))
    schedule.every(365).days.do(lambda: send_email(generate_report("yearly"), "yearly"))

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    report_file = generate_report("yearly")
    send_email(report_file, "yearly")

    monthly_report = generate_report("monthly")
    send_email(monthly_report, "monthly")

    schedule_reports()
