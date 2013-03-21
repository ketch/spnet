import cherrypy
from jinja2 import Environment, FileSystemLoader
import urllib
from datetime import datetime, timedelta

def redirect(path='/', body=None, delay=0):
    'redirect browser, if desired after showing a message'
    s = '<HTML><HEAD>\n'
    s += '<meta http-equiv="Refresh" content="%d; url=%s">\n' % (delay, path)
    s += '</HEAD>\n'
    if body:
        s += '<BODY>%s</BODY>\n' % body
    s += '</HTML>\n'
    return s

def people_link_list(people, maxNames=2):
    l = []
    for p in people[:maxNames]:
        l.append('<A HREF="%s">%s</A>' % (p.get_local_url(), p.name))
    s = ','.join(l)
    if len(people) > maxNames:
        s += ' and %d others' (len(people) - maxNames)
    return s

timeUnits = (('seconds', timedelta(minutes=1), lambda t:int(t.seconds)),
             ('minutes', timedelta(hours=1), lambda t:int(t.seconds / 60)),
             ('hours', timedelta(1), lambda t:int(t.seconds / 3600)),
             ('days', timedelta(7), lambda t:t.days))

monthStrings = ('Jan.', 'Feb.', 'Mar.', 'Apr.', 'May', 'Jun.', 'Jul.',
                'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.')

def display_datetime(dt):
    'get string that sidesteps timezone issues thus: 27 minutes ago'
    def singularize(i, s):
        if i == 1:
            return s[:-1]
        return s
    diff = datetime.utcnow() - dt
    for unit, td, f in timeUnits:
        if diff < td:
            n = f(diff)
            return '%d %s ago' % (n, singularize(n, unit))
    return '%s %d, %d' % (monthStrings[dt.month - 1], dt.day, dt.year)

def timesort(stuff, cmpfunc=lambda x,y:cmp(x.published,y.published),
             reverse=True, **kwargs):
    'sort items by timestamp, most recent first by default'
    l = list(stuff)
    l.sort(cmpfunc, reverse=reverse, **kwargs)
    return l

#################################################################
# template loading and rendering

def get_template_env(dirpath):
    loader = FileSystemLoader(dirpath)
    return Environment(loader=loader)

class TemplateView(object):
    exposed = True
    def __init__(self, template, name=None, **kwargs):
        self.template = template
        self.kwargs = kwargs
        self.name = name

    def __call__(self, doc=None, **kwargs):
        f = self.template.render
        kwargs.update(self.kwargs)
        if doc is not None:
            kwargs[self.name] = doc
        try:
            return f(kwargs=kwargs, hasattr=hasattr, enumerate=enumerate,
                     urlencode=urllib.urlencode, list_people=people_link_list,
                     getattr=getattr, str=str,
                     user=cherrypy.session.get('person', None),
                     display_datetime=display_datetime, timesort=timesort,
                     **kwargs) # apply template
        except Exception, e:
            cherrypy.log.error('view function error', traceback=True)
            cherrypy.response.status = 500
            return 'server error'

##################################################################

class MultiplePages(object):
    'Interface for paging through result sets'
    def __init__(self, f, block_size, ipage, uri, **queryArgs):
        self.f = f
        self.pages = []
        self.block_size = block_size
        self.queryArgs = queryArgs
        self.uri = uri
        self.results = () # default: no results
        self.get_page(ipage, uri, **queryArgs)
    def get_page(self, ipage, uri, **queryArgs):
        '''returns True if we can serve the specified query;
        otherwise False.  Raises StopIteration'''
        if self.uri != uri or queryArgs != self.queryArgs:
            return False # a different search!
        self.error = '' # default: no error
        while ipage >= len(self.pages):
            l = self.f(start=ipage * self.block_size,
                       block_size=self.block_size, **self.queryArgs)
            if l:
                self.pages.append(l)
            if len(l) < self.block_size:
                self.totalPages = len(self.pages)
                if not self.pages:
                    self.error = 'No results matched your query.'
                    return True # report these search results
                if not l:
                    self.error = 'There are no more results matching your query.'
                ipage = len(self.pages) - 1 # last page
                break
        self.ipage = ipage
        self.start = ipage * self.block_size + 1
        self.results = self.pages[ipage]
        self.end = self.start + len(self.results) - 1
        return True # report these search results
    def get_page_url(self, step=1):
        'get URL for page incremented by step'
        return self.uri + '?' + \
               urllib.urlencode(dict(ipage=self.ipage + step,
                                     **self.queryArgs))


