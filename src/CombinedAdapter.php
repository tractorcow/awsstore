<?php

namespace TractorCow\AWSStore;

use SilverStripe\Assets\Flysystem\ProtectedAdapter;
use SilverStripe\Assets\Flysystem\PublicAdapter;

/**
 * An adapter that supports both public and private assets
 */
interface CombinedAdapter extends PublicAdapter, ProtectedAdapter
{

}
