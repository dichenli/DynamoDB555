package DynamoDB;

/**
 * example from http://www.javacodegeeks.com/2013/08/amazon-dynamodb.html
 */
import java.util.Set;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
// TODO: Auto-generated Javadoc

/**
 * The Class CatalogItem.
 */
@DynamoDBTable(tableName="ProductCatalog")
public class CatalogItem {
    
    /** The id. */
    private Integer id;
    
    /** The title. */
    private String title;
    
    /** The isbn. */
    private String ISBN;
    
    /** The book authors. */
    private Set bookAuthors;

    /**
     * Gets the id.
     *
     * @return the id
     */
    @DynamoDBHashKey(attributeName="Id")
    public Integer getId() { return id; }
    
    /**
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(Integer id) { this.id = id; }

    /**
     * Gets the title.
     *
     * @return the title
     */
    @DynamoDBAttribute(attributeName="Title")
    public String getTitle() { return title; }    
    
    /**
     * Sets the title.
     *
     * @param title the new title
     */
    public void setTitle(String title) { this.title = title; }

    /**
     * Gets the isbn.
     *
     * @return the isbn
     */
    @DynamoDBAttribute(attributeName="ISBN")
    public String getISBN() { return ISBN; }    
    
    /**
     * Sets the isbn.
     *
     * @param ISBN the new isbn
     */
    public void setISBN(String ISBN) { this.ISBN = ISBN;}

    /**
     * Gets the book authors.
     *
     * @return the book authors
     */
    @DynamoDBAttribute(attributeName = "Authors")
    public Set getBookAuthors() { return bookAuthors; }    
    
    /**
     * Sets the book authors.
     *
     * @param bookAuthors the new book authors
     */
    public void setBookAuthors(Set bookAuthors) { this.bookAuthors = bookAuthors; }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
       return "Book [ISBN=" + ISBN + ", bookAuthors=" + bookAuthors
       + ", id=" + id + ", title=" + title + "]";            
    }
}
