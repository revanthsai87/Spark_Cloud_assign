import shutil
def save(d3,name):
 try:
    shutil.rmtree(name)
 except PermissionError:
    print("Directory does not exist")
 except OSError:
    print("Issue with rrmdir")
 d3.saveAsTextFile(name)