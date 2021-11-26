import json
import os
import streamlit_wordcloud as wordcloud
import streamlit as st


def load_json(filename) -> dict:
    file = os.path.dirname(os.path.abspath(__file__)) + '/data/' + filename + ".json"
    json_keywords = open(file, 'r')
    data = json_keywords.read()
    json_keywords.close()
    return json.loads(data)


def plot(file: str):
    plot_json = load_json(file)
    word_bags = []
    for key in plot_json:
        value = {"text": key, "value": plot_json[key]}
        word_bags.append(value)

    wordcloud.visualize(word_bags, tooltip_data_fields={
        'text': 'Keywords', 'value': 'Total occurrences'
    }, per_word_coloring=False, palette='coolwarm')


if __name__ == '__main__':
    st.set_page_config(page_title='Data Engineering Weekly NGram Analytics', layout="centered")

    st.title("Data Engineering Weekly Analytics")

    st.sidebar.title("Select the type of analytics you want to see")

    domain_analytics_check = st.sidebar.checkbox('Domain Name Analytics', value=True, key=3)
    url_analytics_check = st.sidebar.checkbox('URL Analytics', value=False, key=1)
    content_analytics_check = st.sidebar.checkbox('Blog Content Analytics', value=False, key=2)

    if domain_analytics_check:
        st.markdown("""
            ## Domain Analytics:
            
            ### How it is done?
            The analysis is simply extracting the domain name from the URL. Most of the engineering blogs hosted on medium.com.
            If the blog hosted on medium.com, we take the first part of the url path. 
            If free text path combined and considered as Independent blog.
        """)

        domain_extractor_code = """
        result: dict[str, int] = dict()
        for url in urls:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            if "medium" in domain:
                domain = parsed_url.path.strip('/').split('/')[0].replace('-', ' ')
    
            domain = domain.lower()
            if domain in result:
                result[domain] += 1
            else:
                result[domain] = 1
        """
        st.code(domain_extractor_code, language='python')

        plot('domain_extractor')

    if url_analytics_check:
        st.markdown("""
            ## URL Analytics
            
            ### How it is done?
            The URL NGram analytics is from observing the URL formation in all the articles shared in the data engineering weekly.
            We noticed that the last part of the URL is usually a description of the articles. The logic for the URL analytics is
            
            1. Get all the data engineering weekly newsletter links
            
            2. For each data engineering weekly newsletter, extract all the links shared for that week.
            
            3. Extract the last part of the URL
        """)

        url_extractor_code = '''
        def extract(self, url: str) -> str:
            splitter = url.strip('/').split('/')
            return splitter[len(splitter) - 1].replace('-', ' ')
        '''

        st.code(url_extractor_code, language='python')

        st.markdown("""
            ### 1 Gram:
        """)
        plot('url_extractor1')

        st.markdown("""
                ### 2 Gram:
            """)
        plot('url_extractor2')

    if content_analytics_check:
        st.markdown("""
                    ## Blog Content Analytics

                    ### How it is done?
                    The blog content analytics extract all the content of the articles, and run through NGram analytics. The logic for the Blog content analytics is
                    
                    1. Get all the data engineering weekly newsletter links

                    2. For each data engineering weekly newsletter, extract all the links shared for that week.

                    3. For all the blog url, extract the content from the html page and sanitize the content
                    
                    4. Run through NGram analytics for the content
                """)

        content_extractor_code = """
            def extract(self, url: str) -> str:
                response = requests.get(url)
                html_page = response.content
                soup = BeautifulSoup(html_page, 'html.parser')
                text = soup.find_all(text=True)
                output = ''
                deny_list = [
                    '[document]',
                    'noscript',
                    'header',
                    'html',
                    'meta',
                    'head',
                    'input',
                    'script',
                    'style',
                    'link',
                    'button',
                    'img',
                ]
        
                for t in text:
                    if t.parent.name not in deny_list:
                        output += '{} '.format(t)
                return output
        """
        st.code(content_extractor_code, language='python')

        st.markdown("""
                   ### 1 Gram:
               """)
        plot('content_extractor1')

        st.markdown("""
                       ### 2 Gram:
                   """)
        plot('content_extractor2')

        st.markdown("""
                         ### 3 Gram:
                    """)
        plot('content_extractor3')
