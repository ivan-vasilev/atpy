import unittest
from atpy.ml.ml_phase import *
from atpy.util.events_util import *
import numpy as np
from atpy.ml.algo_phase import *
import tensorflow as tf


class XorTest(unittest.TestCase):
    """
    XOR Test
    """

    def test_xor(self):
        global_listeners = GlobalListeners()

        # network definition
        nb_classes = 2
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, nb_classes],
                                name="output")
        nb_hidden_nodes = 4
        # enc = tf.one_hot([0, 1], 2)
        w1 = tf.Variable(tf.random_uniform([2, nb_hidden_nodes], -1, 1, seed=0),
                         name="Weights1")
        w2 = tf.Variable(tf.random_uniform([nb_hidden_nodes, nb_classes], -1, 1,
                                           seed=0),
                         name="Weights2")
        b1 = tf.Variable(tf.zeros([nb_hidden_nodes]), name="Biases1")
        b2 = tf.Variable(tf.zeros([nb_classes]), name="Biases2")
        activation2 = tf.sigmoid(tf.matmul(input_, w1) + b1)
        hypothesis = tf.nn.softmax(tf.matmul(activation2, w2) + b2)
        cross_entropy = -tf.reduce_sum(target * tf.log(hypothesis))
        train_step = tf.train.GradientDescentOptimizer(0.1).minimize(cross_entropy)

        correct_prediction = tf.equal(tf.argmax(hypothesis, 1), tf.argmax(target, 1))
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        # Start training
        init = tf.initialize_all_variables()
        with tf.Session() as sess:
            sess.run(init)

            @after
            def xor_data_provider(phase):
                return BaseDataEvent({input_: np.array([[0, 0], [0, 1], [1, 0], [1, 1]]),
                                      target: np.array([[0, 1], [1, 0], [1, 0], [0, 1]])}, phase)

            xor_data_provider += GlobalListeners()

            # training phase
            AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=MLPhase.TRAINING, default_listeners=global_listeners)

            # testing phase
            AlgoPhase(model=lambda x: accuracy.eval(feed_dict=x), phase=MLPhase.TESTING, default_listeners=global_listeners)

            @after
            def start_testing_listener(event):
                if isinstance(event, AfterIterationEvent) and event.phase == MLPhase.TRAINING and event.iteration == 1000:
                    return xor_data_provider(MLPhase.TESTING)
                elif isinstance(event, AfterIterationEvent) and event.phase == MLPhase.TESTING:
                    self.assertEqual(event.model_output, 1)

            start_testing_listener += global_listeners
            global_listeners += start_testing_listener

            # running
            for i in range(1000):
                xor_data_provider(MLPhase.TRAINING)

if __name__ == '__main__':
    unittest.main()
