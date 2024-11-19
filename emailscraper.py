import requests
from bs4 import BeautifulSoup
import re
from http.cookiejar import DefaultCookiePolicy
import logging
import string
import random
from filehandler import FileHandler
from requests.adapters import HTTPAdapter, Retry
import multiprocessing

logger = logging.getLogger(__name__)


class EmailScraper:
    def __init__(self, proxies=None):
        self.proxies = proxies
        self.headers = FileHandler.readHeadersTxtFile("headers.txt")
        self.keywords_to_exclude_from_urls = FileHandler.readKeywordsGivenTxt(
            "keywordsUrl.txt"
        )
        self.keywords_to_exclude_from_emails = FileHandler.readKeywordsGivenTxt(
            "keywordsEmail.txt"
        )
        self.session()

    def session(self):
        """This function creates a session object for requests.

        Returns:
            session - requests.Session = session object
        """
        self.session = requests.Session()
        self.session.cookies.set_policy(DefaultCookiePolicy(rfc2965=True))

        retries = Retry(
            total=1, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_companies_urls_from_duckduck_browser_request(self, company_name):
        """This function extracts urls from DuckDuckGo search engine.

        Args:
            company_name - string = name of the company

        Returns:
            urls_extracted_from_markups - list = list of the most accurate urls for the given company, if the request is not blocked
        """

        search_url = f"https://duckduckgo.com/html/?q={company_name} kontakt"
        try:
            if self.proxies:
                plain_html_code_from_get_request = self.session.get(
                    search_url,
                    headers=random.choice(self.headers),
                    timeout=10,
                    proxies={"http": self.proxies, "https": self.proxies},
                )
            else:
                plain_html_code_from_get_request = self.session.get(
                    search_url, headers=random.choice(self.headers), timeout=10
                )
        except requests.exceptions.ProxyError as e:
            logger.error(f"Proxy Error:\n {e}")
            print("Proxy Error")
            return

        html_code_response_after_parsing = BeautifulSoup(
            plain_html_code_from_get_request.text, "lxml"
        )

        markups_a_from_html_code = html_code_response_after_parsing.find_all(
            "a", href=True, class_="result__a"
        )

        urls_extracted_from_markups = [
            markup["href"] for markup in markups_a_from_html_code
        ]

        if urls_extracted_from_markups == []:
            logger.error(f"Error: DuckDuckGo is blocking the request")
            raise Exception("Error: DuckDuckGo is blocking the request")
        else:
            logger.debug(f"Links found for {company_name}")
        urls_extracted_from_markups = self.clear_browsed_urls_from_particular_keywords(
            urls_extracted_from_markups, self.keywords_to_exclude_from_urls
        )
        return urls_extracted_from_markups

    def scrap_email_url_regex(self, company_url, emails_before_domain_adjusting):
        try:
            if self.proxies:
                plain_html_code_from_get_request = self.session.get(
                    company_url,
                    headers=random.choice(self.headers),
                    timeout=10,
                    proxies={"http": self.proxies, "https": self.proxies},
                )
            else:
                plain_html_code_from_get_request = self.session.get(
                    company_url, headers=random.choice(self.headers), timeout=10
                )

            logger.info(f"Checking for emails in: {company_url}")
            emails_before_domain_adjusting += re.findall(
                r"[\w.+-]+@[\w-]+\.[\w.-]+", plain_html_code_from_get_request.text
            )
            emails_before_domain_adjusting += re.findall(
                r"[\w.+-]+\(at\)[\w-]+\.[\w.-]+", plain_html_code_from_get_request.text
            )
        # zmienic ta lichwe na cos bardziej sensownego
        except requests.exceptions.ProxyError as e:
            logger.error(f"Proxy Error:\n {e}")
            print("Proxy Error")
        except requests.exceptions.Timeout as e:
            print("Timeout")
            logger.error(f"Timeout Error:\n {e}")
        except requests.exceptions.SSLError as e:
            logger.error(f"SSL Error:\n {e}")
        except requests.exceptions.ContentDecodingError as e:
            logger.error(f"Content Decoding Error:\n {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection Error:\n {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error:\n {e}")
        except requests.exceptions.RetryError as e:
            logger.error(f"Retry Error:\n {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error:\n {e}")
        except Exception as e:
            logger.error(f"Error:\n {e}")

    def get_emails_from_companies_urls_using_regex(self, companies_urls):
        """This function extracts emails from the given urls using regex.

        Args:
            companies_urls - list = list of urls

        Returns:
            emails_with_adjusted_domain - list = list of emails from the given urls
        """
        procs = []
        manager = multiprocessing.Manager()
        emails_before_domain_adjusting = manager.list()

        for index_of_companies_urls_list, company_url in enumerate(companies_urls):
            if (
                index_of_companies_urls_list > 0
                and companies_urls[index_of_companies_urls_list - 1] == company_url
            ):
                continue

            proc = multiprocessing.Process(
                target=self.scrap_email_url_regex,
                args=(company_url, emails_before_domain_adjusting),
            )
            procs += proc
            proc.start()

        for proc in procs:
            proc.join()

        emails_with_adjusted_domain = [
            self.adjust_email_domain(email.replace("(at)", "@"))
            for email in emails_before_domain_adjusting
            if self.adjust_email_domain(email.replace("(at)", "@"))
        ]

        if len(emails_with_adjusted_domain) > 0:
            logger.info(f"Emails found")
            return emails_with_adjusted_domain
        else:
            return []

    def adjust_email_domain(self, email_to_adjust):
        """This function checks if the domain of the email is in the list of domains. If not, it replaces the domain with the most similar one from the list. If the domain is not in the list, it returns -1.

        Args:
            email_to_adjust - string = email address

        Returns:
            email - string = email address with the domain changed to the most similar one from the list
            None = if the domain is not in the list or email contains unacceptable keywords
        """

        if any(
            keyword in email_to_adjust
            for keyword in self.keywords_to_exclude_from_emails
        ):
            return None

        acceptable_domains = [
            "com",
            "pl",
            "net",
            "org",
            "eu",
            "de",
            "it",
            "cz",
            "sk",
            "lt",
        ]

        domain_of_email_with_possible_mistake = email_to_adjust.split(".")[-1]

        if domain_of_email_with_possible_mistake in acceptable_domains:
            return str(email_to_adjust).strip()

        for acceptable_domain in acceptable_domains:
            if str(domain_of_email_with_possible_mistake).find(acceptable_domain) != -1:
                email_with_adjusted_domain = (
                    str(email_to_adjust)
                    .replace(domain_of_email_with_possible_mistake, acceptable_domain)
                    .strip()
                )
                email_with_adjusted_domain_and_username = "".join(
                    char
                    for i, char in enumerate(email_with_adjusted_domain)
                    if char in string.ascii_letters
                    or i > 0
                    and email_with_adjusted_domain[i - 1] in string.ascii_letters
                )
                return email_with_adjusted_domain_and_username
            else:
                continue

        return None

    @staticmethod
    def clear_browsed_urls_from_particular_keywords(
        companies_urls_before_clear, keywords_to_exclude
    ):
        """This function clears the list of urls from the urls containing particular keywords.

        Args:
            companies_urls_before_clear - list = list of urls
            keywords_to_exclude - list = list of keywords

        Returns:
            cleared_companies_urls - list = list of unique urls without the urls containing particular keywords
        """
        cleared_companies_urls = [
            company_url
            for company_url in companies_urls_before_clear
            if not any(
                banned_keyword in company_url for banned_keyword in keywords_to_exclude
            )
        ]
        return list(set(cleared_companies_urls))[:6]
