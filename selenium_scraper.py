from selenium import webdriver

links = ['https://www.redfin.com/stingray/api/gis?al=1&min_stories=1&num_homes=10500&page_number=1&sf=1,2,3,5,6,7&sold_within_days=36500&status=9&uipt=1,2,3,4,5,6&user_poly=-71.1436006173%2042.3436686675%2C-71.1436006176%2042.3436686675%2C-71.14376061760001%2042.3435186677%2C-71.14376061770001%2042.3435186675%2C-71.1436006173%2042.3436686675&v=8',
         'https://www.redfin.com/stingray/api/gis?al=1n&min_stories=1&num_homes=10500&ord=redfin-recommended-asc&page_number=1&sf=1,2,3,5,6,7&status=9&uipt=1,2,3,4,5,6&user_poly=-71.1436006173%2042.3436686675%2C-71.1436006176%2042.3436686675%2C-71.14376061760001%2042.3435186677%2C-71.14376061770001%2042.3435186675%2C-71.1436006173%2042.3436686675&v=8']

class SeleniumScraper:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path="./chromedriver.exe")
    def getPageSource(self, link):
        self.driver.get(link)
        return self.driver.page_source
        

