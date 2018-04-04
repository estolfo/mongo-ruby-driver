require 'forwardable'
require 'mongo/operations/specifiable'
require 'mongo/operations/command'
require 'mongo/operations/aggregate'
require 'mongo/operations/result'
require 'mongo/operations/collections_info'
require 'mongo/operations/list_collections'


module Mongo
  module Operations

    # The q field constant.
    #
    # @since 2.1.0
    Q = 'q'.freeze

    # The u field constant.
    #
    # @since 2.1.0
    U = 'u'.freeze

    # The limit field constant.
    #
    # @since 2.1.0
    LIMIT = 'limit'.freeze

    # The multi field constant.
    #
    # @since 2.1.0
    MULTI = 'multi'.freeze

    # The upsert field constant.
    #
    # @since 2.1.0
    UPSERT = 'upsert'.freeze

    # The collation field constant.
    #
    # @since 2.4.0
    COLLATION = 'collation'.freeze

    # The array filters field constant.
    #
    # @since 2.5.0
    ARRAY_FILTERS = 'arrayFilters'.freeze

    # The operation time field constant.
    #
    # @since 2.5.0
    OPERATION_TIME = 'operationTime'.freeze

    # The cluster time field constant.
    #
    # @since 2.5.0
    CLUSTER_TIME = '$clusterTime'.freeze
  end
end
