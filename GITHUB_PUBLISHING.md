# Publishing to GitHub

Here's how to publish this project to GitHub:

## Step 1: Create a GitHub Repository

1. Go to [GitHub.com](https://github.com) and sign in
2. Click the "+" button in the top right corner
3. Select "New repository"
4. Name it something like "lending-club-etl-pipeline"
5. Make it public or private (your choice)
6. Don't initialize with README (we already have one)
7. Click "Create repository"

## Step 2: Initialize Git and Push

Open your terminal/command prompt and run these commands:

```bash
# Navigate to your project folder
cd lending_club_etl

# Initialize git repository
git init

# Add all files
git add .

# Make your first commit
git commit -m "Initial commit: LendingClub ETL pipeline"

# Add the remote repository (replace with your actual URL)
git remote add origin https://github.com/YOUR_USERNAME/lending-club-etl-pipeline.git

# Push to GitHub
git branch -M main
git push -u origin main
```

## Step 3: Update README

After pushing, go to your GitHub repository and:

1. Click on the README.md file
2. Click the edit button (pencil icon)
3. Update the clone URL in the README to match your repository
4. Commit the changes

## Step 4: Add a Description

On your GitHub repository page:

1. Click "About" section
2. Add a description like: "ETL pipeline for processing LendingClub loan data using PySpark and DuckDB"
3. Add topics: etl, pyspark, duckdb, data-engineering, lending-club

## Step 5: Optional - Add a License

If you want to add a license:

1. Go to your repository on GitHub
2. Click "Add file" > "Create new file"
3. Name it "LICENSE"
4. Choose a license (MIT is popular for open source)
5. Commit the file

## Step 6: Test the Setup

After publishing, test that someone else can use your project:

1. Clone it to a different folder
2. Follow the setup instructions
3. Make sure everything works

## Tips for a Good GitHub Repository

- Keep the README clear and simple
- Add screenshots if you have any
- Update the repository description
- Use meaningful commit messages
- Add issues and pull request templates if needed

## Common Issues

**Permission denied**: Make sure you're logged into the correct GitHub account

**Repository not found**: Double-check the repository URL

**Push rejected**: Make sure you have write access to the repository

That's it! Your project should now be live on GitHub and ready for others to use and contribute to.
