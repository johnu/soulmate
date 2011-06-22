module Soulmate

  class LoadItem < Base

    def load(items)
      items_loaded = 0
      items.each_with_index do |item, i|
        id    = item["id"]
        term  = item["term"]
        score = item["score"]

        if id and term
          # store the raw data in a separate key to reduce memory usage
          Soulmate.redis.hset(database, id, JSON.dump(item))

          phrase = ([term] + (item["aliases"] || [])).join(' ')
          prefixes_for_phrase(phrase).uniq.each do |p|
            Soulmate.redis.sadd(base, p) # remember this prefix in a master set
            Soulmate.redis.zadd("#{base}:#{p}", score, id) # store the id of this term in the index
          end
          items_loaded += 1
        end
        puts "added #{i} entries" if i % 100 == 0 and i != 0
      end

      items_loaded
    end
  end
end