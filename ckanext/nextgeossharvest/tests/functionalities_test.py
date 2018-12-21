harvest_url = 'https://micka.lesprojekt.cz/csw?service=CSW&version=2.0.2&request=GetRecords&TYPENAMES=record&sortby=title:A&MaxRecords=1&StartPosition=4'
print harvest_url
splitted_url = harvest_url.split('StartPosition')
print splitted_url[0]
new_position = '101'
new_url = splitted_url[0]+'StartPosition='+new_position
print new_url

a=1
b=None
c=2

new_resources = [x for x in [a, b, c] if x]

print new_resources


a = 12.32

b= 14.9648

print int(a)
print int(b)