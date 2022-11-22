from datetime import datetime
from datetime import timedelta

t_list = ['2022-11-16T00:00:00', '2022-11-17T00:00:00', '2022-11-18T00:00:00']

# Creating an iterator with 3 values in the list
#  And having the list be 1500 elements long. Each
#  of those elements are one iterator of 3 values
i = zip(*([iter(t_list)] * 1500))
new_list = list(map(list, zip(*([iter(t_list)] * 1500))))
print(new_list)
# i = [iter(t_list)] * 5
i = zip(*([iter(t_list)] * 5))
print(list(i))
# print(len(i))

# m = map(list, i)
# print(m)
# # print(len(m))

# l = list(m)
# print(l)
# print(next(i[0]))
# print(next(i[1]))
# print(i[0])
# print(i[1])
# print(i[2])


# i = zip(*([iter(t_list)] * 1500))
# new_list = list(map(list, i))


# new_list = list(map(list, zip(*([iter(original_list)] * split_interval))))
# new_len = len(new_list) * split_interval
# org_len = len(original_list)
# diff = org_len - new_len
# new_list.append(original_list[-diff:])
# return new_list
