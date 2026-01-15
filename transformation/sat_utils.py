def orbit_classifier(meanMotion, eccentricity):

    if meanMotion > 11.25:
        return 'LEO'

    elif (0.99 <= meanMotion <= 1.01) and eccentricity < 0.01:
        return 'GEO'

    elif 0.99 <= meanMotion <= 11.25:
        return 'MEO'

    else:
        return 'HEO'

def launch_year(objectId):

    launchYear = int(objectId.split('-')[0])
    return launchYear



OWNER_KEYWORDS = {
    'SpaceX':     ['STARLINK'],
    'Eutelsat':   ['ONEWEB', 'EUTELSAT'],
    'Amazon':     ['KUIPER'],
    'Iridium':    ['IRIDIUM'],
    'Planet':     ['FLOCK', 'TANAGER', 'PELICAN'],
    'Globalstar': ['GLOBALSTAR'],
    'US Gov':     ['GPS', 'USA', 'GOES', 'SDA', 'PRAETORIAN'],
    'ESA':        ['GALILEO', 'SENTINEL'],
    'Russia':     ['COSMOS', 'GLONASS', 'METEOR', 'SOYUZ', 'LUCH'],
    'Italy':      ['IRIDE'],
    'China':      [
        'BEIDOU', 'FENGYUN', 'YAOGAN', 'TIANHUI', 'SHIYAN', 
        'ZIYUAN', 'HULIANWANG', 'SHENZHOU', 'SHIJIAN', 'CHUTIAN', 
        'QIANFAN', 'GEESAT', 'YUNYAO', 'TIANQI', 'JILIN'
    ]
}

def get_owner(name):

    object_name = name.upper()
    
    for owner, keywords in OWNER_KEYWORDS.items():
        if any(keyword in object_name for keyword in keywords):
            return owner

    return 'Other'