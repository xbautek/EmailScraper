import logging
import pandas as pd
import multiprocessing
import keyboard
import os
from emailscraper import EmailScraper
from dotenv import load_dotenv
import datetime

x = datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
logging.basicConfig(
    filename=f"logs/my_app_{open('timeOfExec.txt','r').read()}.log",
    level=logging.DEBUG,
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)

logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    try:
        load_dotenv()
        username = os.getenv("USERNAME_SMARTPROXY")
        password = os.getenv("PASSWORD_SMARTPROXY")
    except Exception as e:
        print("Error: Environment variables not set or wrong\n")
        logger.critical("Error: Environment variables not set\n" + str(e))
        return
    proxy = f"http://{username}:{password}@pl.smartproxy.com:20000"
    scraper = EmailScraper(proxy)

    try:
        companies_dataframe = pd.read_csv(
            "CompaniesEmails.csv",
            encoding="cp1250",
            sep=";",
            index_col=0,
            encoding_errors="ignore",
        )
        logger.debug("CompaniesEmails.csv read successfully")

        pd.DataFrame.to_csv(
            companies_dataframe,
            f"backup/CompaniesEmailsBackup_{open('timeOfExec.txt','r').read()}.csv",
            sep=";",
            encoding="cp1250",
        )
        logger.debug("CompaniesEmails.csv backup created successfully")

    except Exception as e:
        print("Error: Reading CompaniesEmails.csv")
        logger.error("Error: Reading CompaniesEmails.csv\n" + str(e))
        return

    print("Press CTRL+Q to stop the process\n")

    try:
        for index_of_df_row, row_from_df in companies_dataframe.iterrows():
            if row_from_df["Checked"] == 1:
                continue

            pd.DataFrame.to_csv(
                companies_dataframe,
                "CompaniesEmails.csv",
                sep=";",
                encoding="cp1250",
            )

            company_name_from_csv = row_from_df["Company_name"].replace("&", "")
            print(
                f"\nSearching emails for {company_name_from_csv}, index: {index_of_df_row}"
            )

            try:
                companies_urls_extracted_from_request = (
                    scraper.get_companies_urls_from_duckduck_browser_request(
                        company_name_from_csv
                    )
                )
            except Exception as e:
                print("Error: Request is blocked")
                logger.critical("Error: Request is blocked\n" + str(e))
                break

            if companies_urls_extracted_from_request == []:
                print("Error: Lack of urls")
                logger.warning("Error: Lack of urls")
                companies_dataframe.loc[index_of_df_row, "Checked"] = 1
                continue

            # assign urls to dataframe
            for index_of_urls_list, url_from_urls_list in enumerate(
                companies_urls_extracted_from_request[:3]
            ):
                iterated_url_column_name_in_dataframe = f"Url {index_of_urls_list+1}"
                companies_dataframe.loc[
                    index_of_df_row, iterated_url_column_name_in_dataframe
                ] = url_from_urls_list

            emails_from_given_urls = scraper.get_emails_from_companies_urls_using_regex(
                companies_urls_extracted_from_request
            )
            emails_from_given_urls = sorted(list(set(emails_from_given_urls))[:50])

            if emails_from_given_urls == []:
                print("Lack of emails")
                companies_dataframe.loc[index_of_df_row, "Checked"] = 1
                continue
            else:
                print("Emails were found: ")
                companies_dataframe.loc[index_of_df_row, "Checked"] = 1

            # assign emails to dataframe
            for index_of_email_list, email_from_emails_list in enumerate(
                emails_from_given_urls
            ):
                print(
                    f"index of email = {index_of_email_list+1},  email = {email_from_emails_list}"
                )

                try:
                    iterated_email_column_name = f"Email {index_of_email_list+1}"
                    companies_dataframe.loc[
                        index_of_df_row, iterated_email_column_name
                    ] = email_from_emails_list
                except:
                    companies_dataframe[
                        iterated_email_column_name
                    ] = ""  # creating new column
                    companies_dataframe.loc[
                        index_of_df_row, iterated_email_column_name
                    ] = email_from_emails_list

            if emails_from_given_urls:
                companies_dataframe.loc[index_of_df_row, "Email_found"] = 1

    except Exception as e:
        print("Error occurred during processing")
        logger.error("Error occurred during processing\n" + str(e))
    finally:
        try:
            pd.DataFrame.to_csv(
                companies_dataframe, "CompaniesEmails.csv", sep=";", encoding="cp1250"
            )
            print("Saved to CompaniesEmails.csv")
            logger.debug("Saved to CompaniesEmails.csv")
        except Exception as e:
            print("Error: Saving to CompaniesEmails.csv")
            logger.error("Error: Saving to CompaniesEmails.csv\n" + str(e))


if __name__ == "__main__":
    with open("timeOfExec.txt", mode="w") as date_file:
        date_file.write(str(datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")))
    p = multiprocessing.Process(target=main)
    p.start()
    logger.debug("Process started...")
    try:
        while p.is_alive():
            if keyboard.is_pressed("ctrl+q"):
                for proc in multiprocessing.active_children():
                    proc.terminate()
                p.terminate()
                print("Process stopped...")
                logger.debug("Process stopped...")
                break
    except KeyboardInterrupt:
        p.terminate()
        print("Process interrupted...")
        logger.debug("Process interrupted...")
    finally:
        for proc in multiprocessing.active_children():
            proc.join()
            proc.close()
        p.join()
        p.close()
