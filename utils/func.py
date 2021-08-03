import bs4
import datetime


def xpath_soup(element):
    # type: (typing.Union[bs4.element.Tag, bs4.element.NavigableString]) -> str
    """
    Generate xpath from BeautifulSoup4 element.
    :param element: BeautifulSoup4 element.
    :type element: bs4.element.Tag or bs4.element.NavigableString
    :return: xpath as string
    :rtype: str
    Usage
    -----
    # >>> import bs4
    # >>> html = (
    # ...     '<html><head><title>title</title></head>'
    # ...     '<body><p>p <i>1</i></p><p>p <i>2</i></p></body></html>'
    # ...     )
    # >>> soup = bs4.BeautifulSoup(html, 'html.xdata_parser')
    # >>> xpath_soup(soup.html.body.p.i)
    '/html/body/p[1]/i'
    # >>> import bs4
    # >>> xml = '<doc><elm/><elm/></doc>'
    # >>> soup = bs4.BeautifulSoup(xml, 'lxml-xml')
    # >>> xpath_soup(soup.doc.elm.next_sibling)
    '/doc/elm[2]'
    """
    components = []
    child = element if element.name else element.parent
    for parent in child.parents:  # type: bs4.element.Tag
        siblings = parent.find_all(child.name, recursive=False)
        components.append(
            child.name if 1 == len(siblings) else '%s[%d]' % (
                child.name,
                next(i for i, s in enumerate(siblings, 1) if s is child)
                )
            )
        child = parent
    components.reverse()
    return '/%s' % '/'.join(components)


def delta_date_str(current_data_str, delta_days, is_before):

    current_date_obj = datetime.datetime.strptime(current_data_str, "%Y-%m-%d")
    if is_before:
        delta_obj = (current_date_obj - datetime.timedelta(days=int(delta_days))).strftime("%Y-%m-%d")
    else:
        delta_obj = (current_date_obj + datetime.timedelta(days=int(delta_days))).strftime("%Y-%m-%d")

    return str(delta_obj).strip()


def extract_number(original_str, remove_keyword):
    number_str = original_str.strip().replace(remove_keyword, "").replace(remove_keyword[:-1], "").replace(",", "").strip()
    try:
        if number_str.find("K") >= 0:
            return int(float(number_str.replace("K", "").strip()) * 1000)
        elif number_str.find("M") >= 0:
            return int(float(number_str.replace("M", "").strip()) * 1000000)
        else:
            return int(number_str)
    except Exception as e:
        print(e)
        print(number_str)
        return 1
