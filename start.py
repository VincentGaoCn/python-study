height = 1.86
weight = 90

bmi = weight / (height ** 2)

if bmi < 18.5:
    print("过轻")
elif 18.5 <= bmi < 25:
    print("正常")
elif 25 <= bmi < 30:
    print(" 过重")
else:
    print(" 严重肥胖")

print('bmi : ', bmi)
