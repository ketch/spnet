import re
import core
import errors
from datetime import datetime
import bulk

#################################################################
# hashtag processors
def get_paper(ID, paperType):
    if paperType == 'arxiv':
        return core.ArxivPaperData(ID, insertNew='findOrInsert').parent
    elif paperType == 'pubmed':
        try: # eutils horribly unreliable, handle its failure gracefully
            return core.PubmedPaperData(ID, insertNew='findOrInsert').parent
        except errors.TimeoutError:
            raise KeyError('eutils timed out, unable to retrive pubmedID')
    elif paperType == 'DOI':
        return core.DoiPaperData(DOI=ID, insertNew='findOrInsert').parent
    elif paperType == 'shortDOI':
        return core.DoiPaperData(shortDOI, insertNew='findOrInsert').parent
    else:
        raise Exception('Unrecognized paperType')

#################################################################
# process post content looking for #spnetwork tags
def hashtag_to_spnetID(s, subs=((re.compile('([a-z])_([a-z])'), r'\1-\2'),
                                (re.compile('([0-9])_([0-9])'), r'\1.\2'))):
    'convert 1234_5678 --> 1234.5678 and gr_qc_12345 --> gr-qc_12345'
    for pattern, replace in subs:
        s = pattern.sub(replace, s)
    return s

def get_hashtag_arxiv(m):
    arxivID = hashtag_to_spnetID(str(m))
    return arxivID

def get_arxiv_paper(m):
    arxivID = str(m).replace('/', '_')
    return arxivID

ref_patterns = [('#arxiv_([a-z0-9_]+)', 'arxiv', get_hashtag_arxiv),
                ('ar[xX]iv:\s?[a-zA-Z.-]+/([0-9]+\.[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('ar[xX]iv:\s?([a-zA-Z.-]+/[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('ar[xX]iv:\s?([0-9]+\.[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('http://arxiv.org/[abspdf]{3}/[a-zA-Z.-]+/([0-9]+\.[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('http://arxiv.org/[abspdf]{3}/([a-zA-Z.-]+/[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('http://arxiv.org/[abspdf]{3}/([0-9]+\.[0-9]+v?[0-9]+)','arxiv',get_arxiv_paper),
                ('#pubmed_([0-9]+)', 'pubmed', lambda m:str(m)),
                ('PMID:\s?([0-9]+)', 'pubmed', lambda m:str(m)),
                ('#shortDOI_([a-zA-Z0-9]+)', 'shortDOI', lambda m:str(m)),
                (r'[dD][oO][iI]:(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?!["&\'])\S)+)\b', 'DOI', lambda m:str(m)),
                ('shortDOI:\s?([a-zA-Z0-9]+)', 'shortDOI', lambda m:str(m))]

topic_patterns = ['#([a-zA-Z][a-zA-Z0-9_]+)']
valid_tags = ['recommend', 'discuss', 'announce', 'mustread']

def get_references_and_tags(content):
    """Process the body of a post.  Return
        - a dictionary with each entry of the form {reference: tag}.  Here
          reference is a reference to a paper and tag is one of
          #recommend #discuss #announce #mustread;
        - and a list of topic tags

        Assumptions:
        - Only one tag can be applied to each reference in a given post
        - The tag for each reference must immediately precede the reference
    """
    references = {}
    topics = []
    primary = None

    from html2text import dehtml
    content = dehtml(content)

    # Match all paper references with a tag in front of them
    for pattern, reftype, patfun in ref_patterns:
        refpat = re.compile('#(\w+)\s+'+pattern)
        ref_matches = refpat.findall(content)
        for reference in ref_matches:
            tag = reference[0]
            ref = patfun(reference[1])
            if tag in valid_tags:
                references[ref] = (tag, reftype)
            elif tag != 'spnetwork':
                print 'invalid tag: ', tag
                references[ref] = ('discuss', reftype)

    # Match all paper references without a tag in front of them
    for pattern, reftype, patfun in ref_patterns:
        refpat = re.compile(pattern)
        ref_matches = refpat.findall(content)
        for ref in ref_matches:
            ref = patfun(ref)
            if ref not in references.keys():
                references[ref] = ('discuss', reftype)

    # Now find topic tags
    for pattern in topic_patterns:
        topicpat = re.compile(pattern)
        topics = topicpat.findall(content)
        topics = [t for t in topics if t not in valid_tags]
        topics = [t for t in topics if t != 'spnetwork']
        # Remove duplicates
        topics = list(set(topics))

    # Now find location of #spnetwork and figure out which is the first reference after it
    # This seems a bit wasteful, but shouldn't be a bottleneck
    # Of course, we could check above for the simplest case
    sptagloc = re.compile('#spnetwork').search(content).start()
    first_ref_loc = len(content)
    for pattern, reftype, patfun in ref_patterns:
        ref = re.compile(pattern).search(content)
        if ref is not None:
            refloc = ref.start()
            if (refloc > sptagloc) and (refloc < first_ref_loc):
                first_ref_loc = refloc
                primary = patfun(ref.group(1))


    return references, topics, primary

# replace this by class that queries for ignore=1 topics just once,
# keeps cache
def screen_topics(topicWords, skipAttr='ignore', **kwargs):
    'return list of topic object, filtered by the skipAttr attribute'
    l = []
    for t in topicWords:
        topic = core.SIG.find_or_insert(t, **kwargs)
        if not getattr(topic, skipAttr, False):
            l.append(topic)
    return l



def get_topicIDs(topics, docID, timestamp, source):
    'return list of topic IDs for a post, saving to db if needed'
    topics = screen_topics(topics,
                           origin=dict(source=source, id=docID),
                           published=timestamp)
    return [t._id for t in topics] # IDs for storing to db, etc.


def find_or_insert_posts(posts, get_post_comments, find_or_insert_person,
                         get_content, get_user, get_replycount,
                         get_id, get_timestamp, is_reshare, source,
                         process_post=None, process_reply=None,
                         recentEvents=None, maxDays=None,
                         citationType='discuss', citationType2='discuss',
                         get_title=lambda x:x['title']):
    'generate each post that has a paper hashtag, adding to DB if needed'
    now = datetime.utcnow()
    saveEvents = []
    for d in posts:
        post = None
        timeStamp = get_timestamp(d)
        if maxDays is not None and (now - timeStamp).days > maxDays:
            break
        if is_reshare(d): # just a duplicate (reshared) post, so skip
            continue
        content = get_content(d)
        # Only process posts with #spnetwork in them
        if content.lower().find('#spnetwork')==-1:
            continue
        isRec = content.find('#recommend') >= 0 or \
                content.find('#mustread') >= 0
        isAnn = content.find('#announce') >= 0
        try:
            post = core.Post(get_id(d))
            if getattr(post, 'etag', None) == d.get('etag', ''):
                yield post
                continue # matches DB record, so nothing to do
        except KeyError:
            pass
        refs, topics, primary = get_references_and_tags(content)
        if (post is None) and (refs != {}): # extract data for saving post to DB
            primary_paper_ID = refs[primary]
            primary_paper = get_paper(primary,refs[primary][1])

            userID = get_user(d)
            author = find_or_insert_person(userID)
            d['author'] = author._id
            d['citationType'] = refs[primary][0]
            d['text'] =  content
            if process_post:
                process_post(d)
            d['sigs'] = get_topicIDs(topics, get_id(d),
                                     timeStamp, source)
            post = core.Post(docData=d, parent=primary_paper)
            if len(refs.keys()) > 1: # save 2ary citations
                for ref, meta in refs.iteritems():
                    if ref != primary_paper_ID:
                        paper = get_paper(ref, meta[1])
                        post.add_citations([paper], meta[0])
            try:
                topicsDict
            except NameError:
                topicsDict, subsDict = bulk.get_people_subs()
            bulk.deliver_rec(paper._id, d, topicsDict, subsDict)
            if recentEvents is not None: # add to monitor deque
                saveEvents.append(post)
        elif post is not None: # update DB with new data and etag
            post.update(d)
        yield post
        if get_replycount(d) > 0:
            for c in get_post_comments(get_id(d)):
                if process_reply:
                    process_reply(c)
                try:
                    r = core.Reply(get_id(c))
                    if getattr(r, 'etag', None) != c.get('etag', ''):
                        # update DB record with latest data
                        r.update(dict(etag=c.get('etag', ''),
                                      text=get_content(c),
                                      updated=c.get('updated', '')))
                    continue # already stored in DB, no need to save
                except KeyError:
                    pass
                userID = get_user(c)
                author = find_or_insert_person(userID)
                c['author'] = author._id
                c['text'] =  get_content(c)
                c['replyTo'] = get_id(d)
                if isRec: # record the type of post
                    c['sourcetype'] = 'rec'
                elif isAnn: # record the type of post
                    c['sourcetype'] = 'ann'
                else:
                    c['sourcetype'] = 'post'
                r = core.Reply(docData=c, parent=post._parent_link)
                if recentEvents is not None: # add to monitor deque
                    saveEvents.append(r)

    if saveEvents and recentEvents is not None:
        saveEvents.sort(lambda x,y:cmp(x.published, y.published))
        for r in saveEvents:
            recentEvents.appendleft(r) # add to monitor deque

def test_hashtag_parsing():

    content1 = """<a class="ot-hashtag"
     href="https://plus.google.com/s/%23spnetwork">#spnetwork</a>  <a
     class="ot-hashtag"
     href="https://plus.google.com/s/%23recommend">#recommend</a>
     doi:10.1007/BF01389633<br /><br /><a class="ot-hashtag"
     href="https://plus.google.com/s/%23discuss">#discuss</a>
     doi:10.1147/rd.112.0215<br /><a class="ot-hashtag"
     href="https://plus.google.com/s/%23discuss">#discuss</a>
     doi:10.1007/BF01932030<br /><a class="ot-hashtag"
     href="https://plus.google.com/s/%23numericalAnalysis">#numericalAnalysis</a>"""

    refs, topics, primary = get_references_and_tags(content1)
    assert(primary=='10.1007/BF01389633')
    assert(topics==['numericalAnalysis'])
    assert(refs['10.1007/BF01389633'][0]=='recommend')
    assert(refs['10.1147/rd.112.0215'] == ('discuss', 'DOI'))

    content2 = "#spnetwork arxiv:1234.5678<br />"
    refs, topics, primary = get_references_and_tags(content2)
    assert(primary=='1234.5678')

if __name__ == '__main__':
    test_hashtag_parsing()
