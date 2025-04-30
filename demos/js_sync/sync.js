import { AtpAgent } from '@atproto/api'

const agent = new AtpAgent({
  service: 'https://bsky.social'
})

// JavaScript equivalent of Python's breakpoint() is 'debugger' statement
// Add this statement anywhere you want to pause execution (when devtools are open)
// debugger;

async function getCollectionWithRateLimits(repo, collection) {
    try {
        const r = await agent.com.atproto.repo.listRecords({
            repo,
            collection,
        })
        
        // Extract rate limit information from headers
        const headers = r.headers || {}
        const rateLimitInfo = {
            limit: headers['ratelimit-limit'],
            remaining: headers['ratelimit-remaining'],
            reset: headers['ratelimit-reset']
        }
        
        console.log('Rate Limit Info:', rateLimitInfo)
        return { data: r.data, rateLimitInfo }
    } catch (error) {
        console.error(`Error fetching collection ${collection} for repo ${repo}:`, error)
        throw error
    }
}

async function processRepoCollection(repo, collection) {
    console.log(`Processing repo: ${repo}, collection: ${collection}`)
    // Uncomment to pause execution when devtools are open
    // debugger;
    const result = await getCollectionWithRateLimits(repo, collection)
    return result
}

async function main() {
    const baseRepo = 'did:plc:4rlh46czb2ix4azam3cfyzys'
    const collection = 'app.bsky.feed.post'
    
    const numRepos = 250
    // Create an array with the repo repeated 50 times
    const repos = Array(numRepos).fill(baseRepo)
    
    // Process each repo sequentially to respect rate limits
    const results = []
    for (const repo of repos) {
        try {
            const result = await processRepoCollection(repo, collection)
            results.push(result)
            // Add a small delay to avoid hitting rate limits too hard
            await new Promise(resolve => setTimeout(resolve, 100))
        } catch (error) {
            console.error('Error processing repo:', error)
        }
    }
    
    console.log(`Processed ${results.length} repos successfully`)
    
    // Display sample result from the first repo
    if (results.length > 0) {
        console.log('Sample data from first repo:', results[0].data)
    }
}

main()
