class BeforeTrainingIterationEvent(object):
    def __init__(self, model, iteration, input_data):
        """
        :param model:
        :param iteration:
        :param input_data:
        """
        self.model = model
        self.iteration = iteration
        self.input_data = input_data


class AfterTrainingIterationEvent(object):
    def __init__(self, model, iteration, input_data, model_output):
        """
        :param model:
        :param iteration:
        :param input_data:
        """
        self.model = model
        self.iteration = iteration
        self.input_data = input_data
        self.model_output = model_output
