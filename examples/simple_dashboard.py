import time

import pypeliner_workflow.simple_pipeline as ppl


class SayFirst(ppl.Transformation):
    def transform(self, word_list):
        print(word_list[0], end=' ')
        return word_list[1]


class SaySecond(ppl.Transformation):
    def transform(self, last_word):
        print(last_word)


ppl.enable_dashboard()

mypipeline = ppl.SimplePipeline([
    SayFirst(),
    SaySecond()
])

mypipeline.run(['hello', 'world'])

print('Computation done, look at http://localhost:8080/ for more info')

while True:
    time.sleep(10)
