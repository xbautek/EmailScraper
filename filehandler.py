import logging

logger = logging.getLogger(__name__)


class FileHandler:
    @staticmethod
    def readKeywordsGivenTxt(txt_file_name):
        keywords_to_exclude = []

        try:
            with open(txt_file_name, "r") as file:
                keywords_to_exclude = file.readlines()
                return [keyword.rstrip("\n").strip() for keyword in keywords_to_exclude]
        except Exception as e:
            logger.error("Could not read keywords from file," + str(e))
            raise Exception("Error: Could not read keywords from file")

    @staticmethod
    def readHeadersTxtFile(txt_file_name):  # "headers.txt"
        headers_list_to_raffle_for_session = []
        try:

            with open(txt_file_name, mode="r") as headers_file:
                headers_list_to_raffle_for_session = []
                for single_header_from_file in headers_file:
                    parts_of_header = (
                        single_header_from_file.strip().replace('"', "").split(":", 1)
                    )
                    if len(parts_of_header) == 2:
                        headers_list_to_raffle_for_session.append(
                            {parts_of_header[0].strip(): parts_of_header[1].strip()}
                        )
            return headers_list_to_raffle_for_session

        except Exception as e:
            logger.error("Could not read headers from file," + str(e))
            raise Exception("Error: Could not read headers from file")
