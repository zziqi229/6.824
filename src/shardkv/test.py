with open("log.md","r") as fp:
    s=fp.readlines()
with open("log.md","w") as fp:
    for ss in s[1970:]:
        fp.write(ss)
