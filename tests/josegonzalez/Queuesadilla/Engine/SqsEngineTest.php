<?php
namespace josegonzalez\Queuesadilla\Engine;

use Aws\Sqs\SqsClient;
use josegonzalez\Queuesadilla\Engine\SqsEngine;
use josegonzalez\Queuesadilla\FixtureData;
use josegonzalez\Queuesadilla\TestCase;
use Psr\Log\NullLogger;

/**
 * @group sqs
 */
class SqsEngineTest extends TestCase
{
    public function setUp()
    {
        $this->url = getenv('FAKE_SQS_ENDPOINT');
        $this->config = [
            "endpoint" => $this->url,
            "version" =>'latest',
            "credentials" => [
                "key" => 'A1234B567C890',
                "secret" => 'A1234B567C890',
            ],
            "region" => 'us-east-1',
            "queue" => $this->url . '/default'
        ];

        $this->Logger = new NullLogger;
        $this->engineClass = 'josegonzalez\Queuesadilla\Engine\SqsEngine';
        $this->Engine = $this->mockEngine();
        $this->Fixtures = new FixtureData;
        $this->sqsClient($this->config);
        $this->clearEngine();
        $this->createDefultQueue();
    }

    public function tearDown()
    {
        $this->clearEngine();
        unset($this->Engine);
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\SqsEngine::__construct
     */
    public function testConstruct()
    {
        /*
        $Engine = new SqsEngine($this->Logger, []);
        $this->assertNotNull($Engine->connection());

        $Engine = new SqsEngine($this->Logger, $this->url);
        $this->assertNotNull($Engine->connection());

        $Engine = new SqsEngine($this->Logger, $this->config);
        $this->assertNotNull($Engine->connection());
        */
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::connect
     */
    public function testConnect()
    {
        $this->assertTrue($this->Engine->connect());
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::getJobClass
     */
    public function testGetJobClass()
    {
        $this->assertEquals('\\josegonzalez\\Queuesadilla\\Job\\Base', $this->Engine->getJobClass());
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::acknowledge
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::deleteJob
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::protectedMethodCall
     */
    /*
    public function testAcknowledge()
    {
        $this->assertFalse($this->Engine->acknowledge(null));
        $this->assertFalse($this->Engine->acknowledge(false));
        $this->assertFalse($this->Engine->acknowledge(1));
        $this->assertFalse($this->Engine->acknowledge('string'));
        $this->assertFalse($this->Engine->acknowledge(['key' => 'value']));
        $this->assertFalse($this->Engine->acknowledge($this->Fixtures->default['first']));

        $this->assertTrue($this->Engine->push($this->Fixtures->default['first']));
        $job = new \Pheanstalk\Job($this->Engine->lastJobId(), ['queue' => 'default']);
        $this->assertTrue($this->Engine->push($this->Fixtures->other['third']));

        $data = $this->Fixtures->default['first'];
        $data['id'] = $job->getId();
        $data['job'] = $job;
        $this->assertTrue($this->Engine->acknowledge($data));
    }
    */

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::reject
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::deleteJob
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::protectedMethodCall
     */
    /*
    public function testReject()
    {
        $this->assertFalse($this->Engine->reject(null));
        $this->assertFalse($this->Engine->reject(false));
        $this->assertFalse($this->Engine->reject(1));
        $this->assertFalse($this->Engine->reject('string'));
        $this->assertFalse($this->Engine->reject(['key' => 'value']));
        $this->assertFalse($this->Engine->reject($this->Fixtures->default['first']));

        $this->assertTrue($this->Engine->push($this->Fixtures->default['first']));
        $job = new \Pheanstalk\Job($this->Engine->lastJobId(), ['queue' => 'default']);
        $this->assertTrue($this->Engine->push($this->Fixtures->other['third']));

        $data = $this->Fixtures->default['first'];
        $data['id'] = $job->getId();
        $data['job'] = $job;
        $this->assertTrue($this->Engine->reject($data));
    }
    */

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::pop
     */
    public function testPop()
    {
        $this->assertNull($this->Engine->pop('default'));
        $this->assertTrue($this->Engine->push($this->Fixtures->default['first'], 'default'));

        $item = $this->Engine->pop('default');
        $this->assertInternalType('array', $item);
        $this->assertArrayHasKey('class', $item);
        $this->assertArrayHasKey('args', $item);
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::push
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::pop
     */
    public function testPush()
    {
        $this->assertTrue($this->Engine->push($this->Fixtures->default['first'], 'default'));
        $this->assertTrue($this->Engine->push($this->Fixtures->default['second'], [
            
        ]));
        $this->assertTrue($this->Engine->push($this->Fixtures->other['third'], [
            
        ]));
        $this->assertTrue($this->Engine->push($this->Fixtures->default['fourth'], 'default'));

        sleep(2);

        $pop1 = $this->Engine->pop();
        $pop2 = $this->Engine->pop();
        $pop3 = $this->Engine->pop();
        $pop4 = $this->Engine->pop();

        $this->assertNotEmpty($pop1['id']);
        $this->assertNull($pop1['class']);
        $this->assertEmpty($pop1['args']);
        $this->assertNull($pop2);
        $this->assertEquals('yet_another_function', $pop3['class']);
        $this->assertNull($pop4);
    }

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::release
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::protectedMethodCall
     * @covers josegonzalez\Queuesadilla\Utility\Pheanstalk::releaseJob
     */
    /*
    public function testRelease()
    {
        $this->assertTrue($this->Engine->push($this->Fixtures->default['first'], 'default'));

        $item = $this->Engine->pop('default');
        $this->assertInstanceOf('\Pheanstalk\Job', $item['job']);
        $this->assertTrue($this->Engine->release($item, 'default'));
    }
    */

    /**
     * @covers josegonzalez\Queuesadilla\Engine\BeanstalkEngine::queues
     */
    public function testQueues()
    {
        $this->assertEquals(['default'], $this->Engine->queues());
        $this->Engine->push($this->Fixtures->default['first']);
        $this->assertEquals(['default'], $this->Engine->queues());

        $this->createOtherQueue();
        $this->Engine->push($this->Fixtures->other['second'], ['queue' => 'other']);
        $queues = $this->Engine->queues();
        sort($queues);
        $this->assertEquals(['default', 'other'], $queues);
        $this->Engine->pop();
        $this->Engine->pop();
        $queues = $this->Engine->queues();
        sort($queues);
        $this->assertEquals(['default', 'other'], $queues);
    }

    protected function clearEngine()
    {
        $this->resetServer();
    }

    protected function mockEngine($methods = null, $config = null)
    {
        if ($config === null) {
            $config = $this->config;
        }

        return $this->getMockBuilder($this->engineClass)
            ->setMethods($methods)
            ->setConstructorArgs([$this->Logger, $config])
            ->getMock();
    }

    protected function createDefultQueue()
    {
        return $this->createQueue("default");
    }

    protected function createOtherQueue()
    {
        return $this->createQueue("other");
    }

    protected function createQueue($queueName)
    {
        return $this->sqsClient->createQueue([
            'QueueName' => $queueName
        ]);
    }

    protected function sqsClient($config)
    {
        $this->sqsClient = new SqsClient($config);
    }

    protected function resetServer()
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL,$this->url);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        $result = curl_exec($ch);
        curl_close($ch);

        return $result;
    }
}
