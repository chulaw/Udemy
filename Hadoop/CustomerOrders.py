from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomerOrders(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_customerOrders,
                   reducer=self.reducer_sumOrders),
            MRStep(mapper=self.mapper_sortSumOrders,
                   reducer = self.reducer_sortSumOrders)
        ]

    def mapper_customerOrders(self, _, line):
        (customerId, itemId, cost) = line.split(',')
        yield customerId, float(cost)

    def reducer_sumOrders(self, customerId, cost):
        yield customerId, sum(cost)

    def mapper_sortSumOrders(self, customerId, costSum):
        yield costSum, customerId

    def reducer_sortSumOrders(self, costSum, customers):
        for customerId in customers:
            yield customerId, costSum

if __name__ == '__main__':
    CustomerOrders.run()
