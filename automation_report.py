import pandas as pd
import smtplib
import schedule
import time
from email.message import EmailMessage
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize BigQuery client
client = bigquery.Client()
dataset_id = "financial_project"

# Function to fetch data
def fetch_data(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

# Generate Monthly or Yearly Report
def generate_report(report_type="monthly"):
    if report_type == "monthly":
        query = f"SELECT * FROM `{client.project}.{dataset_id}.kpi_yearly_report` WHERE DATE_TRUNC(Date, MONTH) = CURRENT_DATE()"
        filename = "Monthly_Financial_Report.xlsx"
    else:
        query = f"SELECT * FROM `{client.project}.{dataset_id}.kpi_yearly_report` WHERE DATE_TRUNC(Date, YEAR) = CURRENT_DATE()"
        filename = "Yearly_Financial_Report.xlsx"

    df = fetch_data(query)
    df.to_excel(filename, index=False)
    return filename

def send_email(report_file, report_type):
    sender_email = "psspsp7@gmail.com"
    password = ""  # Use App Password, NOT your real password!

    receiver_emails = ["ceo@company.com", "cfo@company.com", "finance.team@company.com"]  # Fake recipients

    subject = f"Automated {report_type.capitalize()} Financial Report"
    body = f"Attached is the {report_type.capitalize()} Financial Report for your review."

    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_emails)
    msg["Subject"] = subject
    msg.set_content(body)

    with open(report_file, "rb") as attachment:
        msg.add_attachment(attachment.read(), maintype="application", subtype="octet-stream", filename=report_file)

    try:
        with smtplib.SMTP_SSL("smtp.example.com", 465) as server:  # Fake SMTP for demo
            server.login(sender_email, password)
            server.send_message(msg)
        print(f"Email sent successfully to {', '.join(receiver_emails)}")
    except Exception as e:
        print(f"Failed to send email: {e}")


# Automate Scheduled Email Sending
def schedule_reports():
    schedule.every().month.do(lambda: send_email(generate_report("monthly"), "monthly"))
    schedule.every().year.do(lambda: send_email(generate_report("yearly"), "yearly"))

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
