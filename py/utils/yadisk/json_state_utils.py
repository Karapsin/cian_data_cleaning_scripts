import json as js
def parse_offer_json(html, 
                     start_json_template = "window._cianConfig['frontend-offer-card'] ="      
    ):

    # we get such error if and only if we have been blocked
    if start_json_template not in html:
        
        if 'cdn.cian.site/frontend/frontend-status-pages/404.svg' in html:
            raise ValueError('Error 404, page not found')

        print(html)
        raise ValueError("Json not found!!!")

    start = html.index(start_json_template) + len(start_json_template)
    end = html.index('</script>', start)
    json_raw = html[start:end].strip()[:-1]

    parsed_json_list = js.loads(json_raw[json_raw.index('concat(') :-1].replace('concat(', ''))

    # many internal json files contain mostly irrelevant tech info
    # we need only one which contains data about the ads
    needed_key = 'defaultState'
    offer_json = list(filter(lambda x: x['key'] == needed_key, 
                             parsed_json_list
                 ))[0]['value']

    return offer_json