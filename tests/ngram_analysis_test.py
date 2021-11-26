from dataengweekly_analysis.ngram_analysis import domain_names


def test_domain_names():
    urls = []
    urls.append('https://m.facebook.com/watch/9445547199/490224945331402')
    urls.append('https://netflixtechblog.com/building-confidence-in-a-decision-8705834e6fd8')
    urls.append('https://medium.com/vimeo-engineering-blog/uncovering-bias-in-search-and-recommendations-751b01d1c874')
    print(domain_names(urls))
    assert 1 == 1
