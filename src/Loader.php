<?php

declare(strict_types=1);

namespace alvin0319\PreChunkGen;

use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\CancelTaskException;
use pocketmine\scheduler\ClosureTask;
use pocketmine\utils\Utils;
use pocketmine\world\format\io\leveldb\ChunkDataKey;
use pocketmine\world\format\io\leveldb\LevelDB;
use pocketmine\world\World;
use function array_shift;
use function count;
use function in_array;
use function is_int;
use function min;

final class Loader extends PluginBase{

	/** @var array<string, array<array{0: int, 1: int}>> */
	private array $queue = [];

	/** @var int[] */
	private array $generationQueue = [];

	protected function onEnable() : void{
		$this->saveDefaultConfig(); //useless, obviously
		$worlds = $this->getConfig()->get("worlds", []);
		if(count($worlds) < 1){
			$this->getLogger()->notice("No worlds are specified in config.yml");
			return;
		}
		$mode = $this->getConfig()->get("gen-type", "spawn");
		if(!in_array($mode, ["spawn", "fixed"])){
			$this->getLogger()->notice("Unknown gen-type $mode, falling back to 'spawn'");
			$mode = "spawn";
		}
		$xRadius = $this->getConfig()->get("x", 100);
		$zRadius = $this->getConfig()->get("z", 100);
		if(!is_int($xRadius) || !is_int($zRadius)){
			$this->getLogger()->notice("x or z is not an integer, falling back to 100");
			$xRadius = 100;
			$zRadius = 100;
		}
		$maxConcurrentGeneration = $this->getConfig()->get("max-concurrent-generation", 8);
		if(!is_int($maxConcurrentGeneration) || $maxConcurrentGeneration < 1){
			$maxConcurrentGeneration = min(4, Utils::getCoreCount(true) - 2);
			$this->getLogger()->notice("Incorrect max-concurrent-generation, auto-detected value is $maxConcurrentGeneration");
		}
		$this->getScheduler()->scheduleTask(new ClosureTask(function() use ($worlds, $mode, $xRadius, $zRadius, $maxConcurrentGeneration) : void{
			$this->getLogger()->notice("Starting pre-generation of chunks...");
			foreach($worlds as $worldName){
				$world = $this->getServer()->getWorldManager()->getWorldByName($worldName);
				if($world === null){
					$this->getLogger()->notice("Unable to pre-generate chunk for world $worldName, the world has not yet been loaded.");
					continue;
				}
				if(!$this->isLevelDBProvider($world)){
					$this->getLogger()->notice("Unknown world provider for world {$world->getFolderName()}, skipping...");
					continue;
				}
				$this->queue[$world->getId()] = [];
				if($mode === "spawn"){
					$spawn = $world->getSpawnLocation();
					$startX = $spawn->getFloorX() - $xRadius;
					$startZ = $spawn->getFloorZ() - $zRadius;
					$endX = $spawn->getFloorX() + $xRadius;
					$endZ = $spawn->getFloorZ() + $zRadius;
				}else{
					$x = $xRadius / 2;
					$z = $zRadius / 2;
					$startX = -$x;
					$startZ = -$z;
					$endX = $x;
					$endZ = $z;
				}
				for($x = $startX; $x <= $endX; ++$x){
					for($z = $startZ; $z <= $endZ; ++$z){
						$this->queue[$world->getId()][] = [$x, $z];
					}
				}
				$this->getScheduler()->scheduleRepeatingTask(new ClosureTask(function() use ($world, $maxConcurrentGeneration) : void{
					if(count($this->queue[$world->getId()]) === 0){
						$this->getLogger()->notice("Finished pre-generating chunks for world {$world->getFolderName()}");
						throw new CancelTaskException();
					}
					$this->scheduleTask($world->getId(), $maxConcurrentGeneration);
				}), 10);
			}
		}));
	}

	private function scheduleTask(int $worldId, int $maxConcurrentGeneration) : void{
		$world = $this->getServer()->getWorldManager()->getWorld($worldId);
		if($world === null){
			return;
		}
		for($i = 0; $i < $maxConcurrentGeneration; $i++){
			if(count($this->queue[$worldId]) === 0){
				break;
			}
			[$x, $z] = array_shift($this->queue[$worldId]);
			if(!$this->chunkExists($world, $x, $z)){
				$world->orderChunkPopulation($x, $z, null);
			}
		}
	}

	private function chunkExists(World $world, int $x, int $z) : bool{
		/** @var \ReflectionProperty[] $levelDBCaches */
		static $levelDBCaches = [];
		/** @var LevelDB $provider */
		$provider = $world->getProvider();
		if(!isset($levelDBCaches[$world->getId()])){
			try{
				$reflection = new \ReflectionClass(LevelDB::class);
				$db = $reflection->getProperty("db");
				$db->setAccessible(true);
				$levelDBCaches[$world->getId()] = $db;
			}catch(\ReflectionException $e){
				$this->getLogger()->error("This should never happen, is this world ({$world->getFolderName()}) has valid LevelDB provider?");
			}
		}
		/** @var \LevelDB $db */
		$db = $levelDBCaches[$world->getId()]->getValue($provider);

		$index = LevelDB::chunkIndex($x, $z);
		$chunkVersionRaw = $db->get($index . ChunkDataKey::NEW_VERSION);
		if($chunkVersionRaw === false){
			$chunkVersionRaw = $db->get($index . ChunkDataKey::OLD_VERSION);
			if($chunkVersionRaw === false){
				return false;
			}
		}
		return true;
	}

	private function isLevelDBProvider(World $world) : bool{
		return $world->getProvider() instanceof LevelDB;
	}
}
