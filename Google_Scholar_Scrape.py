import logging
import os
import random
import re
from datetime import datetime
from datetime import date
from time import sleep
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
from fuzzywuzzy import fuzz
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options


SCHOLAR_URL = r"https://scholar.google.com/citations?view_op=list_works&hl=en&hl=en&user="
log_file_name = f"../logs/GS_Scrape_Log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
            filename=log_file_name,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
LOGGER = logging.getLogger(__name__)
firefox_options = Options()
firefox_options.add_argument("--headless")

DRIVER = None
failures_in_a_row = 0
failed_times_to_restart = 5

def create_driver():
    global DRIVER
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    DRIVER = webdriver.Firefox(options=firefox_options)


def create_connection():
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db = os.getenv("DB_DB")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    try:
        connection = mysql.connector.connect(host=db_host,
                                             database=db,
                                             user=db_user,
                                             password=db_password,
                                             charset="utf8mb4",
                                             collation="utf8mb4_unicode_ci")
        if connection.is_connected():
             ## LOGGER.info("DataBase Connected")
            return connection
    except mysql.connector.Error as e:
        LOGGER.error(f"DataBase Connection Error: {e}")
        return None


def get_staff_statistics_scrape(x_path):
    try:
        all_stats = DRIVER.find_elements(By.XPATH, x_path)
        data = []
        if not all_stats or len(all_stats) < 3:
            LOGGER.warning("Less than 3 stats found on Scholar profile.")
            return []
        for row in all_stats:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) > 0:
                all_data = cells[1].get_attribute("innerHTML")
                last_5_years_data = cells[2].get_attribute("innerHTML")
                data.append((all_data, last_5_years_data))
        return data
    except Exception as e:
        LOGGER.error(f"Error getting staff statistics: {e}")
        return []


def calculate_h_index(citations_list):
    sorted_citations = sorted(citations_list, reverse=True)
    h_index = sum(1 for i, c in enumerate(sorted_citations) if c >= i + 1)
    return h_index


def calculate_i10_index(citations_list):
    return sum(1 for c in citations_list if c >= 10)


def calculate_stats(publications_df):
    current_year = date.today().year

    recent_df = publications_df[
        pd.to_numeric(publications_df['years'], errors='coerce').fillna(0).astype(int) >= (current_year - 5)
    ]

    all_citations = publications_df['citations'].astype(int).tolist()
    recent_citations = recent_df['citations'].astype(int).tolist()
    # LOGGER.info((recent_df[recent_df['citations'].astype(int) >= 10][['titles', 'years', 'citations']]))
    return {
        'h_index': calculate_h_index(all_citations),
        'h_index_5y': calculate_h_index(recent_citations),
        'i10_index': calculate_i10_index(all_citations),
        'i10_index_5y': sum(1 for c in recent_citations if c >= 10)
    }


def is_captcha_page(driver):
    try:
        current_url = driver.current_url
        if "/sorry/" in current_url or "/sorry" in current_url:
            return True

        page_source = driver.page_source.lower()
        if "unusual traffic" in page_source or "please show you're not a robot" in page_source:
            return True

    except Exception as e:
        LOGGER.error(f"Error checking captcha page: {e}")

    return False


def get_publications_scrape(scholarid):
    global failures_in_a_row

    sleep(2)
    full_scholar_url = SCHOLAR_URL + scholarid

    try:
        DRIVER.get(full_scholar_url)
    except Exception as e:
        LOGGER.error(f"Timeout or error loading scholar page for scholarid={scholarid}: {e}")
        failures_in_a_row += 1
        return None, None, None, None
    
    if is_captcha_page(DRIVER):
        LOGGER.error(f"Captcha detected for scholarid={scholarid} — Sleeping 5 minutes...")
        failures_in_a_row += 1
        sleep(300)  # Κάνε pause 5 λεπτά
        return None, None, None, None

    failures_in_a_row = 0

    staff_citations_graph = get_graph_scrape('//*[@id="gsc_rsb_cit"]/div/div[3]/div')
    staff_stats = get_staff_statistics_scrape('//*[@id="gsc_rsb_st"]/tbody/tr')

    while True:
        try:
            buttons = DRIVER.find_elements(By.XPATH, '//*[@id="gsc_bpf_more"]')
            if buttons:
                show_more_button = buttons[0]
                DRIVER.execute_script("arguments[0].scrollIntoView();", show_more_button)
                WebDriverWait(DRIVER, 2).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="gsc_bpf_more"]')))
                show_more_button.click()
            else:
                break
        except TimeoutException:
            break
        except (NoSuchElementException, ElementClickInterceptedException) as e:
            LOGGER.error(f"Error interacting with ShowMore Button: {e}")
            break

    try:
        rows_length = len(DRIVER.find_elements(By.XPATH, '//*[@id="gsc_a_b"]/tr'))
        titles = []
        publications_urls = []
        publications_scholar_ids = []
        citations = []
        years = []

        for i in range(1, rows_length + 1):
            element = DRIVER.find_element(By.XPATH, f'//*[@id="gsc_a_b"]/tr[{i}]/td[1]/a')
            title = element.text if element.text else "NULL"
            publication_url = element.get_attribute('href') if element.get_attribute('href') else None
            citation = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_a_b"]/tr[{i}]/td[2]/a').get_attribute("innerHTML") or None)
            year = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_a_b"]/tr[{i}]/td[3]/span').get_attribute("innerHTML") or None)
            match = re.search(r'citation_for_view=[^:]+:([^&]+)', publication_url)
            if match:
                publication_scholar_id = match.group(1)
                publications_scholar_ids.append(publication_scholar_id)
            else:
                publications_scholar_ids.append(None)

            titles.append(title)
            publications_urls.append(publication_url)
            if citation is None:
                citation = 0
            citations.append(citation)
            if year and int(year) < 1901:
                year = '1901'
            years.append(year)

        # Creating the DataFrame with the desired columns
        publications_df = pd.DataFrame({
            'titles': titles,
            'years': years,
            'urls': publications_urls,
            'citations': citations,
            'publication_scholar_ids': publications_scholar_ids
        })

        return publications_df, rows_length, staff_citations_graph, staff_stats

    except Exception as e:
        LOGGER.error(f"Error on Publications Scraping: {e}")
        return None, None, None, None


def insert_publication(connection, title, year, publication_url, citations, publication_scholar_id):
    cursor = connection.cursor()
    query = ('INSERT INTO publications (publication_title, citations, publication_url, '
             'publication_year, publication_scholar_id) VALUES (%s, %s, %s, %s, %s)')
    try:
        cursor.execute(query, (title, citations, publication_url, year, publication_scholar_id))
        connection.commit()

        publication_id = cursor.lastrowid
        if publication_id is not None:
            return publication_id
        else:
            LOGGER.error(f"Failed to retrieve publication_id after insertion. title: {title}, citations: {citations}, url: {publication_url}, "
                         f"year: {year}, scholarid: {publication_scholar_id}")
            return None
    except mysql.connector.Error as e:
        LOGGER.error(f"Error on Publication Insertion: {e}")
        return None
    finally:
        cursor.close()


def get_staff_id(connection, scholar_id):
    cursor = connection.cursor()
    staff_id = 0
    try:
        query = 'SELECT staff_id FROM staff WHERE scholar_id = %s'
        cursor.execute(query, (scholar_id,))
        staff_row = cursor.fetchone()
        if staff_row:
            staff_id = staff_row[0]
        else:
            LOGGER.error(f"No staff found for scholar_id: {scholar_id}")
            raise Exception(f"No staff found for scholar_id: {scholar_id}")
    except Exception as e:
        LOGGER.error(f"Error on Getting Staff ID: {e}")
    finally:
        cursor.close()
    return staff_id


def get_staff_name(connection, staff_id):
    cursor = connection.cursor()
    query = "SELECT CONCAT(first_name, ' ', last_name) AS name FROM staff WHERE staff_id = %s"
    staff_full_name = ''
    try:
        cursor.execute(query, (staff_id,))
        staff_full_name = cursor.fetchall()
    except mysql.connector.Error as e:
        LOGGER.error(f"Error on Getting Staff Name: {e}")
    finally:
        cursor.close()
    return staff_full_name


def insert_publication_staff(connection, staff_id, publication_id):
    cursor = connection.cursor()
    try:
        query = 'INSERT INTO publications_staff (staff_id, publication_id) VALUES (%s, %s)'
        cursor.execute(query, (staff_id, publication_id))
        connection.commit()
        publication_staff_id = cursor.lastrowid
        return publication_staff_id
    except Exception as e:
        LOGGER.error(f"Error on Publication Staff Insertion: {e}")
    finally:
        cursor.close()


def update_publication_stats(connection, publication_id, authors, journal, publisher, publication_date):
    cursor = connection.cursor()
    query = 'UPDATE publications SET '
    values = []
    i = 0
    if authors != '':
        query += 'authors = %s'
        values.append(authors)
        i = 1
    if journal != '':
        if i == 0:
            query += 'journal = %s'
            i = 1
        else:
            query += ', journal = %s'
        values.append(journal)
    if publisher != '':
        if i == 0:
            query += 'publisher = %s'
            i = 1
        else:
            query += ', publisher = %s'
        values.append(publisher)
    if publication_date != '0000-00-00' and  publication_date:
        if i == 0:
            query += 'publication_date = %s'
        else:
            query += ', publication_date = %s'
        values.append(publication_date)

    query += ' WHERE publication_id = %s'
    values.append(publication_id)

    try:
        cursor.execute(query, tuple(values))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on Publication Stats Insertion: {e}")
    finally:
        cursor.close()


def insert_publication_staff_author_order(connection, publication_staff_id, author_order):
    cursor = connection.cursor()
    try:
        query = 'UPDATE publications_staff SET  author_order = %s WHERE publication_staff_id = %s'
        cursor.execute(query, (author_order, publication_staff_id))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on Publication Staff Author Order Insertion: {e}")
    finally:
        cursor.close()


def get_all_staff(connection):
    cursor = connection.cursor()
    query = "SELECT staff_id, scholar_id, CONCAT(first_name, ' ', last_name) AS name FROM staff"
    all_staff = []
    try:
        cursor.execute(query)
        all_staff = cursor.fetchall()
    except mysql.connector.Error as e:
        LOGGER.error(f"Error on Getting Staff Name: {e}")
    finally:
        cursor.close()
    return all_staff


def get_graph_scrape(x_path):
    graph = []
    citations = DRIVER.find_elements(By.XPATH, x_path + '/a')
    citations_zindex = []
    citations_data = []
    for citation in citations:
        try:
            z_index = int(citation.get_attribute('style').split('z-index:')[-1].split(';')[0])
            citations_zindex.insert(0, z_index)
            citations = citation.find_element(By.TAG_NAME, "span").get_attribute("innerHTML")
            citations_data.insert(0, citations)
        except IndexError:
            LOGGER.error(f"Error on get_graph_scrape")

    years = DRIVER.find_elements(By.XPATH, x_path + '/span')
    years_data = []
    for year in years:
        years_data.insert(0, year.get_attribute("innerHTML"))

    j = 0
    for i in range(1, len(years_data) + 1):
        if j < len(citations_zindex) and i == citations_zindex[j]:
            graph.append((citations_data[j], years_data[i - 1]))
            j += 1
    return graph


def get_publication_stats_scrape(publication_url, staff_name):
    global failures_in_a_row

    try:
        DRIVER.get(publication_url)
    except Exception as e:
        LOGGER.error(f"Timeout or error loading publication URL: {publication_url} — {e}")
        failures_in_a_row += 1
        return '', '', '', '', 0, []  # Γυρνά κενές τιμές για authors, journal, publisher, publication_date, author_order, citations_graph

    failures_in_a_row = 0  # Αν όλα πάνε καλά, μηδένισε τα failures

    DRIVER.execute_script("window.scrollBy(0, 300);")
    sleep(random.randint(2, 4))

    cols = len(DRIVER.find_elements(By.XPATH, '//*[@id="gsc_oci_table"]/div'))
    authors = ''
    author_order = 0
    publication_date = ''
    journal = ''
    publisher = ''
    citations_graph = []

    for i in range(1, cols + 1):
        element = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_oci_table"]/div[{i}]/div[1]')
                   .get_attribute("innerHTML"))

        if element == "Authors":
            tmp = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_oci_table"]/div[{i}]/div[2]')
                   .get_attribute("innerHTML"))
            if tmp:
                authors = tmp
                authors_array = authors.split(', ')
                for order, name in enumerate(authors_array):
                    name_similarity_percentage = fuzz.partial_ratio(staff_name, name)
                    if name_similarity_percentage > 60:
                        author_order = order + 1
        elif element == "Publication date":
            tmp = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_oci_table"]/div[{i}]/div[2]')
                   .get_attribute("innerHTML"))
            if tmp:
                try:
                    if len(tmp.split("/")) == 1 and re.fullmatch(r"\d{4}", tmp):
                        publication_date = datetime.strptime(tmp + "/01/01", "%Y/%m/%d").date()
                    elif len(tmp.split("/")) == 2:
                        publication_date = datetime.strptime(tmp + "/01", "%Y/%m/%d").date()
                    else:
                        publication_date = datetime.strptime(tmp, "%Y/%m/%d").date()
                except ValueError:
                    LOGGER.error(f"Date: {tmp} Out Of Date Range")
                    publication_date = ''
        elif element == "Journal":
            tmp = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_oci_table"]/div[{i}]/div[2]')
                   .get_attribute("innerHTML"))
            if tmp:
                journal = tmp
        elif element == "Publisher":
            tmp = (DRIVER.find_element(By.XPATH, f'//*[@id="gsc_oci_table"]/div[{i}]/div[2]')
                   .get_attribute("innerHTML"))
            if tmp:
                publisher = tmp
        elif element == "Total citations":
            citations_graph = get_graph_scrape('//*[@id="gsc_oci_graph_bars"]')

    return authors, journal, publisher, publication_date, author_order, citations_graph



def insert_publications_citations_per_year(connection, publication_id, year, citations):
    cursor = connection.cursor()
    try:
        query = 'INSERT INTO publication_citations_per_year (publication_id, year, citations) VALUES (%s, %s, %s)'
        cursor.execute(query, (int(publication_id), year, citations))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on publication_citations_per_year Insertion: {e} {publication_id} {year} {citations}")
    finally:
        cursor.close()


def insert_staff_citations_per_year(connection, staff_id, year, citations):
    cursor = connection.cursor()
    try:
        query = 'INSERT INTO staff_citations_per_year (staff_id, year, citations) VALUES (%s, %s, %s)'
        cursor.execute(query, (staff_id, year, citations))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on staff_citations_per_year Insertion: {e}")
    finally:
        cursor.close()


def insert_staff_statistics(connection, staff_id, total_citations, last_5_years_citations, h_index,
                            last_5_years_h_index, i_10_index, last_5_years_i_10_index, h_index_local,
                            last_5_years_h_index_local, i_10_index_local, last_5_years_i_10_index_local):
    cursor = connection.cursor()
    try:
        query = ('INSERT INTO staff_statistics (staff_id, total_citations, last_5_years_citations, '
                 'h_index, last_5_years_h_index, i10_index, last_5_years_i10_index' 
                 'h_index_local, last_5_years_h_index_local, i10_index_local, last_5_years_i10_index_local) '
                 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
        cursor.execute(query, (staff_id, total_citations, last_5_years_citations, h_index, last_5_years_h_index, 
                               i_10_index, last_5_years_i_10_index, h_index_local, last_5_years_h_index_local, 
                               i_10_index_local, last_5_years_i_10_index_local))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on staff_statistics Insertion: {e}")
    finally:
        cursor.close()


def select_all(connection, staff_id):
    cursor = connection.cursor()

    # Προκαθορισμός των μεταβλητών
    staff_statistics = None
    staff_graph = None

    try:
        query = ('SELECT total_citations, last_5_years_citations, h_index, '
                 'last_5_years_h_index, i10_index, last_5_years_i10_index, h_index_local, '
                 'last_5_years_h_index_local, i10_index_local, last_5_years_i10_index_local, h_index_from_graph, '
                 'last_5_years_h_index_from_graph, i10_index_from_graph, last_5_years_i10_index_from_graph '
                 'FROM staff_statistics WHERE staff_id = %s')
        cursor.execute(query, (staff_id,))
        staff_statistics = cursor.fetchall()
    except Exception as e:
        LOGGER.error(f"Error on select_all staff_statistics Select: {e}")

    try:
        query = 'SELECT citations, year FROM staff_citations_per_year WHERE staff_id = %s'
        cursor.execute(query, (staff_id,))
        staff_graph = cursor.fetchall()
    except Exception as e:
        LOGGER.error(f"Error on select_all staff_citations_per_year Select: {e}")

    try:
        query = ('SELECT p.publication_title, p.publication_year, p.publication_url, p.citations, p.publication_scholar_id, '
                 's.author_order, p.authors, p.publication_date, p.journal, p.publisher, p.publication_id '
                 'FROM publications_staff s JOIN publications p ON s.publication_id = p.publication_id '
                 'WHERE staff_id = %s')
        cursor.execute(query, (staff_id,))
        data = cursor.fetchall()
        column_names = ['titles',
                        'years',
                        'urls',
                        'citations',
                        'publication_scholar_ids',
                        'author_order',
                        'authors',
                        'publication_date',
                        'journal',
                        'publisher',
                        'publication_id']
        publications_df = pd.DataFrame(data, columns=column_names)
    except Exception as e:
        LOGGER.error(f"Error on select_all publications Select: {e}")
        publications_df = pd.DataFrame()

    cursor.close()

    return staff_statistics, staff_graph, publications_df


def select_publication_graph(connection, publication_id):
    cursor = connection.cursor()
    publication_graph = []
    try:
        query = 'SELECT citations, year FROM publication_citations_per_year WHERE publication_id = %s'
        cursor.execute(query, (int(publication_id),))
        publication_graph = cursor.fetchall()
    except Exception as e:
        LOGGER.error(f"Error on select_publication_graph: {e}")
    cursor.close()

    return publication_graph


def update_staff_graph(connection, staff_id, year, citations):
    cursor = connection.cursor()
    try:
        query = ('UPDATE staff_citations_per_year '
                 'SET  citations = %s WHERE staff_id = %s AND year = %s')
        cursor.execute(query, (citations, staff_id, year))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on update_staff_graph: {e}")
    finally:
        cursor.close()


def delete_staff_graphs_entry(connection, staff_id, year):
    cursor = connection.cursor()
    try:
        query = ('DELETE FROM staff_citations_per_year '
                 'WHERE staff_id = %s AND year = %s')
        cursor.execute(query, (int(staff_id), year))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on delete_staff_graphs_entry: {e}")
    finally:
        cursor.close()


def update_publication_graph(connection, publication_id, year, citations):
    cursor = connection.cursor()
    try:
        query = ('UPDATE publication_citations_per_year '
                 'SET  citations = %s WHERE publication_id = %s AND year = %s')
        cursor.execute(query, (citations, int(publication_id), year))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on update_publication_graph: {e}")
    finally:
        cursor.close()


def delete_publication_graph_entry(connection, publication_id, year):
    cursor = connection.cursor()
    try:
        query = ('DELETE FROM publication_citations_per_year '
                 'WHERE publication_id = %s AND year = %s')
        cursor.execute(query, (int(publication_id), year))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on delete_publication_graphs_entry: {e}")
    finally:
        cursor.close()


def update_staff_stats(connection, staff_id, index, value):
    cursor = connection.cursor()
    column = ""
    if index == 0:
        column = "total_citations"
    elif index == 1:
        column = "last_5_years_citations"
    elif index == 2:
        column = "h_index"
    elif index == 3:
        column = "last_5_years_h_index"
    elif index == 4:
        column = "i10_index"
    elif index == 5:
        column = "last_5_years_i10_index"
    elif index == 6:
        column = "h_index_local"
    elif index == 7:
        column = "last_5_years_h_index_local"
    elif index == 8:
        column = "i10_index_local"
    elif index == 9:
        column = "last_5_years_i10_index_local"
    elif index == 10:
        column = "h_index_from_graph"
    elif index == 11:
        column = "last_5_years_h_index_from_graph"
    elif index == 12:
        column = "i10_index_from_graph"
    elif index == 13:
        column = "last_5_years_i10_index_from_graph"
    try:
        query = f'UPDATE staff_statistics SET {column} = %s WHERE staff_id = %s'
        cursor.execute(query, (value, staff_id))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"update_staff_stats: {e}")
    finally:
        cursor.close()


def contains_4byte_utf8(string):
    return any(ord(char) > 0xFFFF for char in string)


def update_publication(connection, staff_id, publication_id, author_order_boolean, author_order,
                       title_boolean, title, authors_boolean, authors, date_boolean, publication_date,
                       journal_boolean, journal, publisher_boolean, publisher, citations_boolean, citations):
    cursor = connection.cursor()
    query = 'UPDATE publications SET '
    values = []
    i = 0
    if title_boolean:
        query += 'publication_title = %s'
        values.append(title)
        i = 1
    if authors_boolean:
        if i == 0:
            query += 'authors = %s'
            i = 1
        else:
            query += ', authors = %s'
        values.append(authors)
    if date_boolean:
        if i == 0:
            query += 'publication_date = %s'
            i = 1
        else:
            query += ', publication_date = %s'
        values.append(publication_date)
    if journal_boolean:
        if i == 0:
            query += 'journal = %s'
            i = 1
        else:
            query += ', journal = %s'
        values.append(journal)
    if publisher_boolean:
        if i == 0:
            query += 'publisher = %s'
            i = 1
        else:
            query += ', publisher = %s'
        values.append(publisher)
    if citations_boolean:
        if i == 0:
            query += 'citations = %s'
            i = 1
        else:
            query += ', citations = %s'
        values.append(citations)

    query += ' WHERE publication_id = %s'
    values.append(int(publication_id))
    try:
        cursor.execute(query, tuple(values))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on Publication Update: {e} , {query} , {publication_id} , {staff_id}")

    if author_order_boolean:
        try:
            query = "UPDATE publications_staff SET author_order = %s WHERE staff_id = %s AND publication_id = %s"
            cursor.execute(query, (int(author_order), int(staff_id), int(publication_id)))
            connection.commit()
        except Exception as e:
            LOGGER.error(f"Error on Publication Update Author Order: {e} {publication_id} {staff_id}")

    cursor.close()


def calculate_indices_from_graph(publication_citations_per_year_df):
    current_year = datetime.now().year

    # Συνολικές παραπομπές ανά δημοσίευση
    total_citations = publication_citations_per_year_df.groupby("publication_id")["citations"].sum()

    # Παραπομπές τελευταίας 5ετίας
    last_5_years_df = publication_citations_per_year_df[
        publication_citations_per_year_df["year"] >= current_year - 5
    ]
    citations_last_5 = last_5_years_df.groupby("publication_id")["citations"].sum()

    def h_index(citations):
        citations = sorted(citations, reverse=True)
        return sum(c >= i + 1 for i, c in enumerate(citations))

    def i10_index(citations):
        return sum(c >= 10 for c in citations)

    return {
        "h_index_from_graph": h_index(total_citations.tolist()),
        "i10_index_from_graph": i10_index(total_citations.tolist()),
        "last_5_years_h_index_from_graph": h_index(citations_last_5.tolist()),
        "last_5_years_i10_index_from_graph": i10_index(citations_last_5.tolist()),
    }


def update_staff_stats_bulk(connection, staff_id, update_fields):
    cursor = connection.cursor()
    try:
        query = "UPDATE staff_statistics SET "
        query += ", ".join(f"{field} = %s" for field in update_fields.keys())
        query += " WHERE staff_id = %s"
        values = list(update_fields.values())
        values.append(staff_id)
        cursor.execute(query, tuple(values))
        connection.commit()
    except Exception as e:
        LOGGER.error(f"Error on bulk updating staff_statistics: {e}")
    finally:
        cursor.close()


def main():
    LOGGER.info("-- START PROGRAM --")

    global failures_in_a_row, failed_times_to_restart

    connection = create_connection()
    create_driver()

    all_staff = get_all_staff(connection)
    LOGGER.info(f"Total staff fetched from database: {len(all_staff)}")
    
    for i in range(376, len(all_staff)):
        staff_id = 0
        scholar_id = 0
        full_name = ''
        for j in range(len(all_staff[i])):
            if j == 0:
                staff_id = all_staff[i][j]
            elif j == 1:
                scholar_id = all_staff[i][j]
            else:
                full_name = all_staff[i][j]

        LOGGER.info(f"Processing staff_id={staff_id}, name={full_name}, scholar_id={scholar_id}")
        publications_df_scrape, rows_length, staff_citations_graph_scrape, staff_stats_scrape = get_publications_scrape(
            scholar_id)
        
        if publications_df_scrape is None or staff_stats_scrape is None:
            LOGGER.warning(f"Scraping failed for scholar_id={scholar_id} (staff_id={staff_id})")

            if failures_in_a_row >= failed_times_to_restart:
                LOGGER.warning(f"Restarting WebDriver after {failures_in_a_row} consecutive failures...")
                try:
                    DRIVER.quit()
                except Exception as e:
                    LOGGER.error(f"Error quitting driver: {e}")
                sleep(10)  # Μικρό διάλειμμα
                create_driver()
                failures_in_a_row = 0

            continue

        if not staff_stats_scrape or len(staff_stats_scrape) < 3:
            LOGGER.warning(f"Incomplete Scholar stats for scholar_id={scholar_id} (staff_id={staff_id}), skipping...")
            continue
        
        staff_stats_db, staff_citations_graph_db, publications_df_db = select_all(connection, staff_id)
        
        # STAFF STATISTICS
        publication_ids = publications_df_db["publication_id"].tolist()

        citation_rows = []
        for pub_id in publication_ids:
            graph = select_publication_graph(connection, pub_id)
            for citations, year in graph:
                citation_rows.append({
                    "publication_id": pub_id,
                    "year": int(year),
                    "citations": int(citations)
                })

        if citation_rows:
            citation_df = pd.DataFrame(citation_rows)
            graph_stats = calculate_indices_from_graph(citation_df)

        st_stats_tmp = tuple(int(x) for pair in staff_stats_scrape for x in pair)
        local_stats = calculate_stats(publications_df_scrape)
        local_stats_tuple = (
            local_stats['h_index'],
            local_stats['h_index_5y'],
            local_stats['i10_index'],
            local_stats['i10_index_5y'],
            graph_stats["h_index_from_graph"],
            graph_stats["last_5_years_h_index_from_graph"],
            graph_stats["i10_index_from_graph"],
            graph_stats["last_5_years_i10_index_from_graph"]
        )

        st_stats = st_stats_tmp + local_stats_tuple
        
        if staff_stats_db:
            st_stats_db = tuple(staff_stats_db[0])
        else:
            st_stats_db = []
        LOGGER.info(f"staff_stats_db = {st_stats_db}, staff_stast = {st_stats} !!")
        try:
            if not st_stats_db and st_stats:
                insert_staff_statistics(connection, staff_id, st_stats[0], st_stats[1], st_stats[2],
                                    st_stats[3], st_stats[4], st_stats[5], st_stats[6],
                                    st_stats[7], st_stats[8], st_stats[9], st_stats[10], 
                                    st_stats[11], st_stats[12], st_stats[13])
            elif st_stats != st_stats_db:
                differences = [
                    (i, val1, val2)
                    for i, (val1, val2) in enumerate(zip(st_stats, st_stats_db))
                    if val1 != val2
                ]

                update_fields = {}

                for diff in differences:
                    index, new, old = diff
                    ## LOGGER.info(f"Differences on staff stats: staff_id: {staff_id}, old value: "
                    ##  f"{old}, new value: {new}, in position: {index}")
                    

                    if index == 0:
                        update_fields["total_citations"] = new
                    elif index == 1:
                        update_fields["last_5_years_citations"] = new
                    elif index == 2:
                        update_fields["h_index"] = new
                    elif index == 3:
                        update_fields["last_5_years_h_index"] = new
                    elif index == 4:
                        update_fields["i10_index"] = new
                    elif index == 5:
                        update_fields["last_5_years_i10_index"] = new
                    elif index == 6:
                        update_fields["h_index_local"] = new
                    elif index == 7:
                        update_fields["last_5_years_h_index_local"] = new
                    elif index == 8:
                        update_fields["i10_index_local"] = new
                    elif index == 9:
                        update_fields["last_5_years_i10_index_local"] = new
                    elif index == 10:
                        update_fields["h_index_from_graph"] = new
                    elif index == 11:
                        update_fields["last_5_years_h_index_from_graph"] = new
                    elif index == 12:
                        update_fields["i10_index_from_graph"] = new
                    elif index == 13:
                        update_fields["last_5_years_i10_index_from_graph"] = new
                if update_fields:
                    update_staff_stats_bulk(connection, staff_id, update_fields)
        except IndexError:
            LOGGER.error(f"IndexError inserting stats for staff_id={staff_id}, st_stats={st_stats}, st_stats_length= {len(st_stats)}")
            continue

        # STAFF GRAPH
        graph = [(int(x), int(y)) for x, y in staff_citations_graph_scrape]
        if not staff_citations_graph_db and graph:
            for citations_st, year_st in graph:
                insert_staff_citations_per_year(connection, staff_id, year_st, citations_st)
        elif staff_citations_graph_db != graph:
            set_staff_graph_db = set(staff_citations_graph_db)
            set_staff_graph = set(graph)
            deleted = set_staff_graph_db - set_staff_graph
            # Differences
            diffs = set_staff_graph - set_staff_graph_db
            diffs_list = list(diffs)
            deleted_list = list(deleted)
            ## LOGGER.info(f"Differences on graph: {diffs_list}, Deleted Data: {deleted_list}")
            for citations_x, year_x in diffs_list:
                tmp = 0
                for citations_y, year_y in staff_citations_graph_db:
                    if year_x == year_y:
                        tmp = 1
                        update_staff_graph(connection, staff_id, year_x, citations_x)
                if tmp == 0:
                    insert_staff_citations_per_year(connection, staff_id, year_x, citations_x)
            for citations_x, year_x in deleted_list:
                tmp = 0
                for citations_y, year_y in diffs_list:
                    if year_x == year_y:
                        tmp = 1
                if tmp == 0:
                    delete_staff_graphs_entry(connection, staff_id, year_x)

        if not publications_df_scrape.empty:
            for publication_sc in range(len(publications_df_scrape)):
                exists = False
                for publication_db in range(len(publications_df_db)):
                    if publications_df_scrape['urls'][publication_sc] == publications_df_db['urls'][publication_db]:
                        publication_id = publications_df_db['publication_id'][publication_db]
                        exists = True
                        citations_scrape_to_int = None
                        if publications_df_scrape['citations'][publication_sc] != "NULL":
                            citations_scrape_to_int = int(publications_df_scrape['citations'][publication_sc])
                        if citations_scrape_to_int != publications_df_db['citations'][publication_db]:
                            authors, journal, publisher, pub_date, author_order, citations_graph = \
                                (get_publication_stats_scrape(publications_df_scrape['urls'][publication_sc],
                                                              full_name))
                            if failures_in_a_row >= failed_times_to_restart:
                                LOGGER.warning(f"Restarting WebDriver after {failures_in_a_row} consecutive failures...")
                                try:
                                    DRIVER.quit()
                                except Exception as e:
                                    LOGGER.error(f"Error quitting driver: {e}")
                                sleep(10)
                                create_driver()
                                failures_in_a_row = 0
                            author_order_boolean = False
                            title_boolean = False
                            authors_boolean = False
                            date_boolean = False
                            journal_boolean = False
                            publisher_boolean = False
                            citations_boolean = True
                            if author_order != publications_df_db['author_order'][publication_db]:
                                author_order_boolean = True
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Author Order From: "
                                ## f"{publications_df_db['author_order'][publication_db]} To: {author_order}")
                            if (publications_df_scrape['titles'][publication_sc] !=
                                    publications_df_db['titles'][publication_db]):
                                if publications_df_db['titles'][publication_db] != 'Unknown Title: Non ASCII':
                                    title_boolean = True
                                    if publications_df_scrape['titles'][publication_sc]:
                                        if contains_4byte_utf8(publications_df_scrape['titles'][publication_sc]):
                                            title = 'Unknown Title: Non ASCII'
                                else:
                                    title_boolean = False
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Title From: {publications_df_db['titles'][publication_db]} "
                                ## f"To: {publications_df_scrape['titles'][publication_sc]}")
                            similarity = fuzz.ratio(authors, publications_df_db['authors'][publication_db])
                            if similarity < 80:
                                authors_boolean = True
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Authors From: {publications_df_db['authors'][publication_db]} "
                                ## f"To: {authors}")
                            if publications_df_db['publication_date'][publication_db]:
                                dt = str(publications_df_db['publication_date'][publication_db].strftime("%Y-%m-%d"))
                                if str(pub_date) != dt:
                                    date_boolean = True
                                    if pub_date == '':
                                        pub_date = None
                                    ## LOGGER.info(f"PublicationId: {publication_id} Changed Date From: {dt} To: {pub_date}")
                            if not journal:
                                journal = None
                            if journal != publications_df_db['journal'][publication_db]:
                                journal_boolean = True
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Journal From: {publications_df_db['journal'][publication_db]} "
                                ## f"To: {journal}")
                            if not publisher:
                                publisher = None
                            if publisher != publications_df_db['publisher'][publication_db]:
                                publisher_boolean = True
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Publisher From: {publications_df_db['publisher'][publication_db]}"
                                ## f" To: {publisher}")
                                ## LOGGER.info(f"PublicationId: {publication_id} Changed Citations From: {publications_df_db['citations'][publication_db]} "
                                ## f"To: {citations_scrape_to_int}")
                            update_publication(connection, staff_id, publication_id, author_order_boolean, author_order,
                                               title_boolean, publications_df_scrape['titles'][publication_sc],
                                               authors_boolean, authors, date_boolean, pub_date, journal_boolean,
                                               journal, publisher_boolean, publisher, citations_boolean,
                                               citations_scrape_to_int)
                            publication_graph_db = select_publication_graph(connection, publication_id)
                            if citations_graph:
                                citations_graph_scrape_to_int = [(int(cit), int(year)) for cit, year in citations_graph]
                                if citations_graph_scrape_to_int != publication_graph_db:
                                    set_publication_graph_db = set(publication_graph_db)
                                    set_citations_graph_scrape_to_int = set(citations_graph_scrape_to_int)
                                    deleted = set_publication_graph_db - set_citations_graph_scrape_to_int
                                    # Differences
                                    diffs = set_citations_graph_scrape_to_int - set_publication_graph_db
                                    diffs_list = list(diffs)
                                    deleted_list = list(deleted)
                                    ## LOGGER.info(f"PublicationId: {publication_id} Differences on Publication graph: {diffs_list}, "
                                    ## f"Deleted Data: {deleted_list}")
                                    for citations_x, year_x in diffs_list:
                                        tmp = 0
                                        for citations_y, year_y in publication_graph_db:
                                            if year_x == year_y:
                                                tmp = 1
                                                update_publication_graph(connection, publication_id,
                                                                         year_x, citations_x)
                                        if tmp == 0:
                                            insert_publications_citations_per_year(connection, publication_id,
                                                                                   year_x, citations_x)
                                    for citations_x, year_x in deleted_list:
                                        tmp = 0
                                        for citations_y, year_y in diffs_list:
                                            if year_x == year_y:
                                                tmp = 1
                                        if tmp == 0:
                                            delete_publication_graph_entry(connection, publication_id, year_x)
                if not exists:
                    publication_year = None
                    if publications_df_scrape['years'][publication_sc]:
                        publication_year = publications_df_scrape['years'][publication_sc]
                    title = publications_df_scrape['titles'][publication_sc]
                    if title:
                        if contains_4byte_utf8(title):
                            title = 'Unknown Title: Non ASCII'
                    ## LOGGER.info(f"title: {title}")
                    publication_id = insert_publication(connection, title,
                                                        publication_year,
                                                        publications_df_scrape['urls'][publication_sc],
                                                        int(publications_df_scrape['citations'][publication_sc]),
                                                        publications_df_scrape['publication_scholar_ids'][publication_sc])
                    if publication_id != 'NULL' and publication_id:
                        publication_staff_id = insert_publication_staff(connection, staff_id, publication_id)
                        authors, journal, publisher, pub_date, author_order, citations_graph = \
                            (get_publication_stats_scrape(publications_df_scrape['urls'][publication_sc], full_name))
                        for citation, year in citations_graph:
                            insert_publications_citations_per_year(connection, publication_id, year, citation)
                        if authors or journal or publisher or pub_date:
                            update_publication_stats(connection, publication_id, authors, journal, publisher, pub_date)
                        insert_publication_staff_author_order(connection, publication_staff_id, author_order)
        LOGGER.info(f"Finished processing staff_id={staff_id}")                

    LOGGER.info(f"END PROGRAM")


if __name__ == "__main__":
    main()
